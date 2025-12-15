import os
import json
import asyncio
import logging
import time
from typing import Union, Optional, List
from aiohttp import web, ClientSession 
from pyrogram import Client, filters, enums, idle, types
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
import pyromod

from config import API_ID, API_HASH, BOT_TOKEN, OWNER_ID
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
WORKER_COUNT = 20 

# --- BOT CLIENT ---
app = Client(
    "BackupChannelBot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# --- WORKER LOGIC ---

async def fetch_and_save_batch(bot, chat_id, batch_ids):
    """
    Fetches a batch of messages with NO FILTERS (Saves Everything).
    """
    valid_docs = []
    messages = []
    
    # Retry Loop
    retries = 0
    max_retries = 10
    
    while True:
        try:
            messages = await bot.get_messages(chat_id, batch_ids)
            break 
        except FloodWait as e:
            await asyncio.sleep(e.value + 2)
        except Exception as e:
            retries += 1
            if retries >= max_retries:
                logger.error(f"Dropped batch {batch_ids[0]}-{batch_ids[-1]}: {e}")
                return 0, 0 
            await asyncio.sleep(min(retries, 10))

    if not messages:
        return 0, 0

    for message in messages:
        # Skip only truly empty/service messages
        if not message or message.empty or message.service:
            continue
        
        # Must have ANY media
        if not message.media:
            continue
            
        # Get Media Object (Video, Doc, Audio, Photo, Voice, etc.)
        media_type_str = message.media.value
        media = getattr(message, media_type_str, None)
        if not media:
            continue

        # --- NO EXTENSION FILTER --- 
        # We accept EVERYTHING.
        
        try:
            # Try to unpack file_id. If fails, skip.
            file_id_encoded = unpack_new_file_id(media.file_id)
            
            # Determine filename
            filename = getattr(media, "file_name", None)
            if not filename:
                # Fallback for unnamed media
                filename = f"{media_type_str}_{media.file_unique_id}"
            
            caption = message.caption or ""
            safe_filename = str(filename).replace("_", " ")
            
            doc = {
                '_id': file_id_encoded,
                'file_name': safe_filename,
                'file_size': getattr(media, "file_size", 0),
                'file_id_tg': media.file_id,
                'file_unique_id': media.file_unique_id,
                'message_id': message.id,
                'chat_id': message.chat.id,
                'caption': caption
            }
            valid_docs.append(doc)
        except Exception:
            continue
    
    if valid_docs:
        return await db.save_batch(valid_docs)
    return 0, 0

async def worker(queue, bot, chat_id, stats):
    while True:
        batch_ids = await queue.get()
        if batch_ids is None: 
            queue.task_done()
            break
        
        if CANCEL:
            queue.task_done()
            continue

        try:
            saved, dups = await fetch_and_save_batch(bot, chat_id, batch_ids)
            stats['saved'] += saved
            stats['dups'] += dups
        except Exception:
            pass
        finally:
            stats['processed'] += len(batch_ids)
            queue.task_done()

async def index_files_to_db(last_msg_id: int, chat, msg: Message, bot: Client, start_from: int):
    global CANCEL
    start_time = time.time()
    
    stats = {'saved': 0, 'dups': 0, 'processed': 0}
    
    queue = asyncio.Queue()
    all_ids = list(range(start_from + 1, last_msg_id + 1))
    total_messages = len(all_ids)
    
    if total_messages == 0:
        await msg.edit("⚠️ No messages to index.")
        return

    # Fill Queue
    chunks = [all_ids[i:i + BATCH_SIZE] for i in range(0, len(all_ids), BATCH_SIZE)]
    for chunk in chunks:
        queue.put_nowait(chunk)
        
    for _ in range(WORKER_COUNT):
        queue.put_nowait(None)
    
    await msg.edit(
        f"🚀 **Full Index Started**\n"
        f"Total: `{total_messages}`\n"
        f"Filters: `DISABLED (All Files)`\n"
        f"Mode: `Zero Loss`"
    )
    
    workers = []
    for _ in range(WORKER_COUNT):
        w = asyncio.create_task(worker(queue, bot, chat.id, stats))
        workers.append(w)
    
    # Progress Loop
    while not queue.empty():
        if CANCEL:
            break
        await asyncio.sleep(4) 
        
        time_taken = get_readable_time(time.time() - start_time)
        percent = (stats['processed'] / total_messages) * 100 if total_messages > 0 else 0
        
        try:
            btn = [[InlineKeyboardButton('STOP', callback_data=f'index#cancel')]]
            await msg.edit_text(
                f"🛡️ **Indexing...**\n\n"
                f"Scanned: `{stats['processed']}` / `{total_messages}` ({percent:.1f}%)\n"
                f"Saved: `{stats['saved']}`\n"
                f"Duplicates: `{stats['dups']}`\n"
                f"Time: `{time_taken}`",
                reply_markup=InlineKeyboardMarkup(btn)
            )
        except Exception:
            pass

    await queue.join()
    for w in workers: w.cancel()
    
    time_taken = get_readable_time(time.time() - start_time)
    await msg.edit(
        f'✅ **Complete!**\n'
        f'Scanned: `{stats["processed"]}`\n'
        f'Saved: `{stats["saved"]}`\n'
        f'Duplicates: `{stats["dups"]}`\n'
        f'Time: `{time_taken}`\n\n'
        f'**Sending JSON...**'
    )
    await send_backup(bot, msg.chat.id)

async def send_backup(client, target_chat_id):
    file_path = "channel_backup.json"
    try:
        data = await db.get_all_data()
        if not data:
            return await client.send_message(target_chat_id, "⚠️ No data found.")
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        await client.send_document(
            chat_id=target_chat_id, 
            document=file_path, 
            caption=f"📦 **Backup**\nRecords: `{len(data)}`"
        )
    except Exception as e:
        logger.error(f"Backup error: {e}")
    finally:
        if os.path.exists(file_path): os.remove(file_path)

# --- COMMANDS ---

@app.on_message(filters.command('start') & filters.private)
async def start_command(client, message):
    await message.reply_text("👋 **Zero-Loss Indexer**\nUse `/index` to start.")

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
        chat = await bot.get_chat(chat_id)
    except:
        return await message.reply(f'❌ Cannot access chat.')

    s = await message.reply("Send skip number (0 for none).")
    msg_skip = await bot.listen(chat_id=message.chat.id, user_id=message.from_user.id)
    await s.delete()
    skip = int(msg_skip.text)

    # --- RESET DB OPTION ---
    buttons = [
        [InlineKeyboardButton('YES (CONTINUE)', callback_data=f'index#no#{chat.id}#{last_msg_id}#{skip}')],
        [InlineKeyboardButton('YES (RESET DB)', callback_data=f'index#reset#{chat.id}#{last_msg_id}#{skip}')],
        [InlineKeyboardButton('CLOSE', callback_data='close_data')]
    ]
    await message.reply(
        f'📝 **Index: {chat.title}**\nTotal: `{last_msg_id}`\n\n'
        f'⚠️ **"RESET DB" will delete all existing data!**\nUse this if you see false duplicates.',
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
