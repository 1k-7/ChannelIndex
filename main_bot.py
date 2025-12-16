import os
import json
import asyncio
import logging
import time
import re
from typing import Union, Optional, List
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
WORKER_COUNT = 15 # Balanced speed to avoid "breaking" pipe

# --- BOT CLIENT ---
app = Client(
    "BackupChannelBot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# --- REFERENCE LOGIC WORKER ---

async def fetch_and_process_batch(bot, chat_id, batch_ids):
    """
    Fetches messages and applies index.py logic.
    Returns: (valid_docs, stats_increment)
    """
    valid_docs = []
    # Local stats for this batch
    s = {'deleted': 0, 'no_media': 0, 'unsupported': 0, 'processed': 0}
    
    messages = []
    retries = 0
    
    # Reliable Fetch Loop
    while True:
        try:
            messages = await bot.get_messages(chat_id, batch_ids)
            break 
        except FloodWait as e:
            await asyncio.sleep(e.value + 2)
        except Exception as e:
            retries += 1
            if retries >= 5:
                # If we fail 5 times, assume these IDs are inaccessible or deleted
                s['deleted'] += len(batch_ids) 
                return [], s
            await asyncio.sleep(1)

    if not messages:
        s['deleted'] += len(batch_ids)
        return [], s

    s['processed'] = len(messages)

    for message in messages:
        # 1. Check Empty/Service (Reference Logic)
        if not message or message.empty or message.service:
            s['deleted'] += 1
            continue
        
        # 2. Check Media Existence (Reference Logic)
        if not message.media:
            s['no_media'] += 1
            continue
            
        # 3. Check Media Type (Reference Logic + Audio)
        # index.py checks [VIDEO, DOCUMENT]. We include AUDIO to be safe, but exclude PHOTO.
        if message.media not in [enums.MessageMediaType.VIDEO, enums.MessageMediaType.DOCUMENT, enums.MessageMediaType.AUDIO]:
            s['unsupported'] += 1
            continue

        media = getattr(message, message.media.value, None)
        if not media:
            s['unsupported'] += 1
            continue

        # 4. Check Extensions (Reference Logic)
        # If user wants NO missing files, they must ensure INDEX_EXTENSIONS in config.py is comprehensive.
        file_name = getattr(media, "file_name", "")
        if not file_name:
             # Logic from ia_filterdb.py: fallbacks if needed, or skip?
             # Reference usually skips if no extension match.
             # We will be slightly lenient: if no name but is video, we keep it.
             if message.media == enums.MessageMediaType.VIDEO:
                 file_name = f"video_{media.file_unique_id}.mp4"
             else:
                 s['unsupported'] += 1
                 continue
        
        # Clean filename logic from ia_filterdb.py
        # re.sub(r"@\w+|(_|\-|\.|\+)", " ", str(media.file_name))
        clean_name = re.sub(r"@\w+|(_|\-|\.|\+)", " ", str(file_name))
        
        # Check Extension
        if not str(file_name).lower().endswith(tuple(INDEX_EXTENSIONS)):
            s['unsupported'] += 1
            continue
        
        try:
            # Unpack ID (Reference Logic)
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
            continue
            
    return valid_docs, s

async def worker(queue, bot, chat_id, global_stats):
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
            
            # Update Global Stats
            global_stats['processed'] += s['processed']
            global_stats['deleted'] += s['deleted']
            global_stats['no_media'] += s['no_media']
            global_stats['unsupported'] += s['unsupported']
            
            if docs:
                saved, dups = await db.save_batch(docs)
                global_stats['saved'] += saved
                global_stats['dups'] += dups
                
        except Exception as e:
            logger.error(f"Worker Exception: {e}")
        finally:
            queue.task_done()

async def index_files_to_db(last_msg_id: int, chat, msg: Message, bot: Client, start_from: int):
    global CANCEL
    CANCEL = False
    start_time = time.time()
    
    # Detailed Stats Tracking
    stats = {
        'saved': 0, 'dups': 0, 'processed': 0,
        'deleted': 0, 'no_media': 0, 'unsupported': 0
    }
    
    queue = asyncio.Queue()
    all_ids = list(range(start_from + 1, last_msg_id + 1))
    total_msgs = len(all_ids)
    
    if total_msgs == 0:
        await msg.edit("⚠️ No messages to index.")
        return

    # Fill Queue
    chunks = [all_ids[i:i + BATCH_SIZE] for i in range(0, len(all_ids), BATCH_SIZE)]
    for chunk in chunks:
        queue.put_nowait(chunk)
    for _ in range(WORKER_COUNT):
        queue.put_nowait(None)
    
    await msg.edit(
        f"🚀 **Indexing Started**\n"
        f"Messages: `{total_msgs}`\n"
        f"Workers: `{WORKER_COUNT}`"
    )
    
    workers = [asyncio.create_task(worker(queue, bot, chat.id, stats)) for _ in range(WORKER_COUNT)]
    
    # UI Loop
    while not queue.empty():
        if CANCEL: break
        await asyncio.sleep(5)
        
        time_taken = get_readable_time(time.time() - start_time)
        # Calculate percentage based on Processed + Deleted (Total IDs handled)
        total_handled = stats['processed'] + stats['deleted']
        percent = (total_handled / total_msgs) * 100 if total_msgs > 0 else 0
        
        try:
            btn = [[InlineKeyboardButton('STOP', callback_data=f'index#cancel')]]
            await msg.edit_text(
                f"🛡️ **Status:**\n"
                f"Total Scanned: `{total_handled}` / `{total_msgs}` ({percent:.1f}%)\n"
                f"✅ Saved: `{stats['saved']}`\n"
                f"♻️ Duplicates: `{stats['dups']}`\n"
                f"🗑️ Deleted/Empty: `{stats['deleted']}`\n"
                f"🚫 No Media: `{stats['no_media']}`\n"
                f"⚠️ Unsupported: `{stats['unsupported']}`\n"
                f"Time: `{time_taken}`",
                reply_markup=InlineKeyboardMarkup(btn)
            )
        except Exception:
            pass

    await queue.join()
    for w in workers: w.cancel()
    
    time_taken = get_readable_time(time.time() - start_time)
    await msg.edit(
        f"✅ **Indexing Complete!**\n\n"
        f"Saved: `{stats['saved']}`\n"
        f"Duplicates: `{stats['dups']}`\n"
        f"Deleted/Empty: `{stats['deleted']}`\n"
        f"Other Media: `{stats['unsupported'] + stats['no_media']}`\n"
        f"Time: `{time_taken}`\n\n"
        f"**Preparing Backup...**"
    )
    await send_backup(bot, msg.chat.id)

async def send_backup(client, target_chat_id):
    file_path = "channel_backup.json"
    try:
        data = await db.get_all_data()
        if not data:
            return await client.send_message(target_chat_id, "⚠️ Database is empty.")
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        await client.send_document(
            chat_id=target_chat_id, 
            document=file_path, 
            caption=f"📦 **Backup**\nFiles: `{len(data)}`"
        )
    except Exception as e:
        logger.error(f"Backup error: {e}")
    finally:
        if os.path.exists(file_path): os.remove(file_path)

# --- COMMANDS ---

@app.on_message(filters.command('start') & filters.private)
async def start_command(client, message):
    await message.reply_text("👋 **Reference Logic Indexer**\nUse `/index` to start.")

@app.on_message(filters.command('index') & filters.private & filters.user(OWNER_ID))
async def send_for_index(bot: Client, message: Message):
    if lock.locked():
        return await message.reply('⚠️ Process running.')
    
    i = await message.reply("Forward **last message** or send link.")
    try:
        msg_input = await bot.listen(chat_id=message.chat.id, user_id=message.from_user.id)
    except:
        return
    await i.delete()
    
    chat_id, last_msg_id = None, None
    
    if msg_input.text and "t.me" in msg_input.text:
        parts = msg_input.text.split("/")
        last_msg_id = int(parts[-1])
        chat_id = int(f"-100{parts[-2]}") if parts[-2].isdigit() else parts[-2]
    elif msg_input.forward_from_chat:
        last_msg_id = msg_input.forward_from_message_id
        chat_id = msg_input.forward_from_chat.id
    else:
        return await message.reply('❌ Invalid.')

    try:
        await bot.get_chat(chat_id)
    except:
        return await message.reply(f'❌ Cannot access chat.')

    s = await message.reply("Send skip number (0 for none).")
    msg_skip = await bot.listen(chat_id=message.chat.id, user_id=message.from_user.id)
    await s.delete()
    try:
        skip = int(msg_skip.text)
    except:
        skip = 0

    buttons = [
        [InlineKeyboardButton('YES (CONTINUE)', callback_data=f'index#no#{chat_id}#{last_msg_id}#{skip}')],
        [InlineKeyboardButton('YES (RESET DB)', callback_data=f'index#reset#{chat_id}#{last_msg_id}#{skip}')],
        [InlineKeyboardButton('CLOSE', callback_data='close_data')]
    ]
    await message.reply(
        f'📝 **Index Request**\nTotal Messages: `{last_msg_id}`\n\n'
        f'⚠️ **Check your `INDEX_EXTENSIONS` in config if files are skipped!**',
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@app.on_callback_query(filters.regex(r'^index'))
async def index_callback(bot, query):
    global CANCEL
    data = query.data.split("#")
    
    if data[1] == 'cancel':
        CANCEL = True
        return await query.answer("Stopping...")
        
    action, chat_id, last_msg, skip = data[1], int(data[2]), int(data[3]), int(data[4])
    
    if action == 'reset':
        await db.drop_collection()
        await query.answer("Database Cleared!", show_alert=True)
        
    chat_obj = await bot.get_chat(chat_id)
    await index_files_to_db(last_msg, chat_obj, query.message, bot, skip)

@app.on_callback_query(filters.regex(r'^close_data'))
async def close_callback(bot, query):
    await query.message.delete()

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
