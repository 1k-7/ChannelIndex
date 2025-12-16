import os
import json
import asyncio
import logging
import time
import re
from aiohttp import web, ClientSession 
from pyrogram import Client, filters, enums, idle, types
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
import pyromod

from config import API_ID, API_HASH, BOT_TOKEN, OWNER_ID, INDEX_EXTENSIONS
from db_utils import db, unpack_new_file_id

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logging.getLogger("pyrogram").setLevel(logging.WARNING) 
logger = logging.getLogger(__name__)

# --- WEB SERVER & SELF-PING ---
routes = web.RouteTableDef()

@routes.get("/", allow_head=True)
async def root_route_handler(request):
    return web.json_response({"status": "running", "message": "Bot is active"})

async def web_server():
    web_app = web.Application(client_max_size=30000000)
    web_app.add_routes(routes)
    return web_app

async def auto_ping():
    url = os.environ.get("RENDER_EXTERNAL_URL") 
    if not url: return
    if not url.startswith("http"): url = f"http://{url}"
    while True:
        try:
            async with ClientSession() as session:
                async with session.get(url) as resp:
                    pass
        except:
            pass
        await asyncio.sleep(600)

# --- UTILS ---
def get_readable_time(seconds: int) -> str:
    time_list = [int(seconds / 86400), int(seconds / 3600) % 24, int(seconds / 60) % 60, int(seconds) % 60]
    up_time = ""
    for i, j in enumerate(time_list):
        if j != 0:
            up_time += f"{j}{'d' if i==0 else 'h' if i==1 else 'm' if i==2 else 's'} "
    return up_time.strip() if up_time else '1s'

# --- GLOBALS ---
lock = asyncio.Lock()
CANCEL = False 
BATCH_SIZE = 200
WORKER_COUNT = 15
STATUS_UPDATE_INTERVAL = 20

# Global State
CURRENT_PROCESS = {
    'chat_name': 'None',
    'total_msgs': 0,
    'start_time': 0,
    'active': False,
    'stats': {
        'saved': 0, 'dups': 0, 'processed': 0,
        'deleted': 0, 'no_media': 0, 'unsupported': 0, 'errors': 0
    }
}

app = Client(
    "BackupChannelBot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# --- MEMORY EFFICIENT BACKUP ---
async def send_backup(client, target_chat_id, status_msg=None):
    file_path = "channel_backup.json"
    
    if status_msg:
        await status_msg.edit("📦 **Exporting Data...**\nStreaming data to file (Memory Efficient Mode).")
    else:
        await client.send_message(target_chat_id, "📦 **Starting Export...**")

    try:
        # Check if data exists
        total_docs = await db.total_documents()
        if total_docs == 0:
            return await client.send_message(target_chat_id, "⚠️ Database is empty.")

        # STREAMING WRITE (Low RAM Usage)
        cursor = db.get_cursor()
        
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("[\n") # Start JSON Array
            
            first = True
            count = 0
            
            async for doc in cursor:
                if '_id' in doc: del doc['_id'] # Remove ObjectID
                
                if not first:
                    f.write(",\n")
                
                # Dump single object to string and write immediately
                f.write(json.dumps(doc, ensure_ascii=False))
                
                first = False
                count += 1
                
                # Log progress every 50k
                if count % 50000 == 0:
                    logger.info(f"Exported {count} records...")

            f.write("\n]") # End JSON Array

        file_size = os.path.getsize(file_path) / (1024 * 1024) # MB
        caption = f"📦 **Backup Complete**\nRecords: `{count}`\nSize: `{file_size:.2f} MB`"

        # If file is > 50MB (Telegram Bot Limit for normal send_document),
        # Pyrogram usually handles it, but Render upload speed might timeout.
        # We try standard send.
        
        await client.send_document(
            chat_id=target_chat_id, 
            document=file_path, 
            caption=caption
        )
        
    except Exception as e:
        logger.error(f"Backup error: {e}")
        await client.send_message(target_chat_id, f"❌ Export Failed: `{str(e)}`")
    finally:
        if os.path.exists(file_path): os.remove(file_path)


# --- INDEXING WORKER LOGIC ---

async def fetch_and_process_batch(bot, chat_id, batch_ids):
    valid_docs = []
    s = {'deleted': 0, 'no_media': 0, 'unsupported': 0, 'processed': 0, 'errors': 0}
    
    messages = []
    retries = 0
    
    while True:
        try:
            messages = await bot.get_messages(chat_id, batch_ids)
            break 
        except FloodWait as e:
            await asyncio.sleep(e.value + 2)
        except Exception as e:
            retries += 1
            if retries >= 3:
                s['errors'] += len(batch_ids)
                return [], s
            await asyncio.sleep(1)

    if not messages:
        s['deleted'] += len(batch_ids)
        return [], s

    s['processed'] = len(messages)

    for message in messages:
        if not message or message.empty or message.service:
            s['deleted'] += 1
            continue
        
        if not message.media:
            s['no_media'] += 1
            continue
            
        if message.media not in [enums.MessageMediaType.VIDEO, enums.MessageMediaType.DOCUMENT, enums.MessageMediaType.AUDIO]:
            s['unsupported'] += 1
            continue

        media_type_str = message.media.value
        media = getattr(message, media_type_str, None)
        if not media:
            s['unsupported'] += 1
            continue

        file_name = getattr(media, "file_name", None)
        if not file_name:
             if message.media == enums.MessageMediaType.VIDEO:
                 file_name = f"video_{media.file_unique_id}.mp4"
             elif message.media == enums.MessageMediaType.AUDIO:
                 file_name = f"audio_{media.file_unique_id}.mp3"
             else:
                 s['unsupported'] += 1
                 continue
        
        clean_name = re.sub(r"@\w+|(_|\-|\.|\+)", " ", str(file_name))
        
        if not str(file_name).lower().endswith(tuple(INDEX_EXTENSIONS)):
            s['unsupported'] += 1
            continue
        
        try:
            file_id_encoded = unpack_new_file_id(media.file_id)
            doc = {
                '_id': file_id_encoded,
                'file_name': clean_name.strip(),
                'file_size': getattr(media, "file_size", 0),
                'file_id_tg': media.file_id,
                'file_unique_id': media.file_unique_id,
                'message_id': message.id,
                'chat_id': message.chat.id,
                'caption': (message.caption or "")
            }
            valid_docs.append(doc)
        except Exception:
            s['errors'] += 1
            continue
            
    return valid_docs, s

async def worker(queue, bot, chat_id):
    global CURRENT_PROCESS
    while True:
        batch_ids = await queue.get()
        if batch_ids is None: 
            queue.task_done()
            break
        if CANCEL:
            queue.task_done()
            continue

        try:
            docs, s = await fetch_and_process_batch(bot, chat_id, batch_ids)
            CURRENT_PROCESS['stats']['processed'] += s['processed']
            CURRENT_PROCESS['stats']['deleted'] += s['deleted']
            CURRENT_PROCESS['stats']['no_media'] += s['no_media']
            CURRENT_PROCESS['stats']['unsupported'] += s['unsupported']
            CURRENT_PROCESS['stats']['errors'] += s['errors']
            
            if docs:
                saved, dups = await db.save_batch(docs)
                CURRENT_PROCESS['stats']['saved'] += saved
                CURRENT_PROCESS['stats']['dups'] += dups
        except Exception as e:
            logger.error(f"Worker Exception: {e}")
        finally:
            queue.task_done()

async def index_files_to_db(last_msg_id: int, chat, msg: Message, bot: Client, start_from: int):
    global CANCEL, CURRENT_PROCESS
    CANCEL = False
    
    CURRENT_PROCESS = {
        'chat_name': chat.title,
        'total_msgs': (last_msg_id - start_from),
        'start_time': time.time(),
        'active': True,
        'stats': {
            'saved': 0, 'dups': 0, 'processed': 0,
            'deleted': 0, 'no_media': 0, 'unsupported': 0, 'errors': 0
        }
    }
    
    queue = asyncio.Queue()
    all_ids = list(range(start_from + 1, last_msg_id + 1))
    
    if not all_ids:
        await msg.edit("⚠️ No messages.")
        return

    chunks = [all_ids[i:i + BATCH_SIZE] for i in range(0, len(all_ids), BATCH_SIZE)]
    for chunk in chunks: queue.put_nowait(chunk)
    for _ in range(WORKER_COUNT): queue.put_nowait(None)
    
    await msg.edit(f"🚀 **Indexing {chat.title}**\nMsgs: `{len(all_ids)}`")
    workers = [asyncio.create_task(worker(queue, bot, chat.id)) for _ in range(WORKER_COUNT)]
    
    while not queue.empty():
        if CANCEL: break
        await asyncio.sleep(STATUS_UPDATE_INTERVAL)
        st = CURRENT_PROCESS['stats']
        total = CURRENT_PROCESS['total_msgs']
        done = st['processed'] + st['deleted'] + st['no_media'] + st['unsupported']
        percent = (done / total) * 100 if total > 0 else 0
        try:
            btn = [[InlineKeyboardButton('STOP', callback_data=f'index#cancel')]]
            await msg.edit_text(
                f"🛡️ **Indexing... {percent:.1f}%**\n"
                f"Saved: `{st['saved']}` | Dups: `{st['dups']}`\n"
                f"Scanned: `{done}` / `{total}`",
                reply_markup=InlineKeyboardMarkup(btn)
            )
        except: pass

    await queue.join()
    for w in workers: w.cancel()
    CURRENT_PROCESS['active'] = False
    
    st = CURRENT_PROCESS['stats']
    await msg.edit(
        f"✅ **Done!**\nSaved: `{st['saved']}`\n"
        f"Duplicates: `{st['dups']}`\n"
        f"**Exporting JSON...**"
    )
    await send_backup(bot, msg.chat.id, msg)

# --- COMMANDS ---

@app.on_message(filters.command('export') & filters.private & filters.user(OWNER_ID))
async def export_command(client, message):
    """Manually trigger backup of current DB"""
    await send_backup(client, message.chat.id)

@app.on_message(filters.command('ts') & filters.private & filters.user(OWNER_ID))
async def total_status_command(client, message):
    if not CURRENT_PROCESS['active']: return await message.reply("💤 Idle.")
    st = CURRENT_PROCESS['stats']
    total = CURRENT_PROCESS['total_msgs']
    done = st['processed'] + st['deleted'] + st['no_media'] + st['unsupported']
    await message.reply(
        f"📊 **Status**\n"
        f"Progress: `{done}` / `{total}`\n"
        f"Saved: `{st['saved']}` | Dups: `{st['dups']}`\n"
        f"Unsupported: `{st['unsupported']}`"
    )

@app.on_message(filters.command('start') & filters.private)
async def start_command(client, message):
    await message.reply_text("👋 **Indexer Bot**\n`/index` - Start\n`/export` - Get Backup\n`/ts` - Status")

@app.on_message(filters.command('index') & filters.private & filters.user(OWNER_ID))
async def send_for_index(bot: Client, message: Message):
    if lock.locked(): return await message.reply('⚠️ Busy.')
    i = await message.reply("Forward last msg.")
    try: msg_input = await bot.listen(chat_id=message.chat.id, user_id=message.from_user.id)
    except: return
    await i.delete()
    
    if msg_input.text and "t.me" in msg_input.text:
        parts = msg_input.text.split("/")
        last_msg_id, chat_id = int(parts[-1]), int(f"-100{parts[-2]}") if parts[-2].isdigit() else parts[-2]
    elif msg_input.forward_from_chat:
        last_msg_id, chat_id = msg_input.forward_from_message_id, msg_input.forward_from_chat.id
    else: return await message.reply('❌ Invalid.')

    try: await bot.get_chat(chat_id)
    except: return await message.reply(f'❌ No Access.')

    s = await message.reply("Skip? (0 for none)")
    msg_skip = await bot.listen(chat_id=message.chat.id, user_id=message.from_user.id)
    await s.delete()
    try: skip = int(msg_skip.text)
    except: skip = 0

    buttons = [
        [InlineKeyboardButton('CONTINUE', callback_data=f'index#no#{chat_id}#{last_msg_id}#{skip}')],
        [InlineKeyboardButton('RESET DB', callback_data=f'index#reset#{chat_id}#{last_msg_id}#{skip}')],
        [InlineKeyboardButton('CLOSE', callback_data='close_data')]
    ]
    await message.reply(f'📝 **Index** `{last_msg_id}` msgs?', reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex(r'^index'))
async def index_callback(bot, query):
    global CANCEL
    data = query.data.split("#")
    if data[1] == 'cancel':
        CANCEL = True
        return await query.answer("Stopping...")
    if data[1] == 'reset':
        await db.drop_collection()
        await query.answer("DB Cleared!", show_alert=True)
    await index_files_to_db(int(data[3]), await bot.get_chat(int(data[2])), query.message, bot, int(data[4]))

@app.on_callback_query(filters.regex(r'^close_data'))
async def close_callback(bot, query): await query.message.delete()

if __name__ == "__main__":
    PORT = int(os.environ.get("PORT", 8080))
    app_web = web.Application(client_max_size=30000000)
    app_web.add_routes(routes)
    async def start():
        runner = web.AppRunner(app_web)
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", PORT).start()
        await app.start()
        asyncio.create_task(auto_ping())
        await idle()
        await app.stop()
    asyncio.get_event_loop().run_until_complete(start())
