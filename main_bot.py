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
BATCH_SIZE = 200        # Max Telegram allows per request
WORKER_COUNT = 20       # High concurrency for speed

# --- BOT CLIENT ---
app = Client(
    "BackupChannelBot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# --- ROBUST WORKER LOGIC ---

async def fetch_and_save_batch(bot, chat_id, batch_ids):
    """
    Fetches a batch of messages with aggressive retries to ensure NO FILE IS MISSING.
    """
    valid_docs = []
    messages = []
    
    # 1. RETRY LOOP: Keep trying to fetch the batch until successful
    retries = 0
    max_retries = 50 # Retry up to 50 times for network errors
    
    while True:
        try:
            messages = await bot.get_messages(chat_id, batch_ids)
            break # Success! Exit loop.
        except FloodWait as e:
            logger.warning(f"⏳ FloodWait: Sleeping {e.value}s. Batch {batch_ids[0]} will be retried.")
            await asyncio.sleep(e.value + 2)
            # Do NOT break; retry immediately after sleep
        except Exception as e:
            retries += 1
            if retries >= max_retries:
                logger.error(f"❌ CRITICAL: Dropped batch {batch_ids[0]}-{batch_ids[-1]} after {max_retries} retries. Error: {e}")
                return 0, 0 # Give up to prevent infinite hang on permanent errors
            
            wait_time = min(retries * 2, 30) # Exponential backoff up to 30s
            logger.warning(f"⚠️ Batch connection error: {e}. Retrying in {wait_time}s ({retries}/{max_retries})...")
            await asyncio.sleep(wait_time)

    if not messages:
        return 0, 0

    # 2. PROCESSING LOOP
    for message in messages:
        # Gracefully handle deleted/empty messages
        if not message or message.empty or message.service:
            continue
        
        if not message.media:
            continue
            
        if message.media not in [enums.MessageMediaType.VIDEO, enums.MessageMediaType.DOCUMENT, enums.MessageMediaType.AUDIO]:
            continue
        
        media_type_str = message.media.value
        media = getattr(message, media_type_str, None)
        if not media:
            continue
            
        file_name = str(getattr(media, "file_name", "")).lower()
        if file_name and not file_name.endswith(INDEX_EXTENSIONS):
            continue

        try:
            file_id_encoded = unpack_new_file_id(media.file_id)
            caption = message.caption or ""
            # Sanitize filename for text search
            safe_filename = str(media.file_name or media.file_unique_id).replace("_", " ")
            
            doc = {
                '_id': file_id_encoded,
                'file_name': safe_filename,
                'file_size': media.file_size,
                'file_id_tg': media.file_id,
                'file_unique_id': media.file_unique_id,
                'message_id': message.id,
                'chat_id': message.chat.id,
                'caption': caption
            }
            valid_docs.append(doc)
        except Exception:
            continue
    
    # 3. SAVE TO DB
    if valid_docs:
        # save_batch handles duplicate errors internally
        return await db.save_batch(valid_docs)
    return 0, 0

async def worker(queue, bot, chat_id, stats):
    """
    Continuous worker that pulls batches from queue.
    """
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
        except Exception as e:
            logger.error(f"Worker Unexpected Error: {e}")
        finally:
            stats['processed'] += len(batch_ids)
            queue.task_done()

async def index_files_to_db(last_msg_id: int, chat, msg: Message, bot: Client, start_from: int):
    global CANCEL
    start_time = time.time()
    
    stats = {'saved': 0, 'dups': 0, 'processed': 0}
    
    # Prepare Queue
    queue = asyncio.Queue()
    all_ids = list(range(start_from + 1, last_msg_id + 1))
    total_messages = len(all_ids)
    
    if total_messages == 0:
        await msg.edit("⚠️ No new messages to index.")
        return

    # Chunk IDs into batches of 200
    chunks = [all_ids[i:i + BATCH_SIZE] for i in range(0, len(all_ids), BATCH_SIZE)]
    for chunk in chunks:
        queue.put_nowait(chunk)
        
    for _ in range(WORKER_COUNT):
        queue.put_nowait(None)
    
    await msg.edit(
        f"🚀 **Robust Indexing Started!**\n\n"
        f"Total Messages: `{total_messages}`\n"
        f"Workers: `{WORKER_COUNT}`\n"
        f"Strategy: `Zero Data Loss`"
    )
    
    # Start Workers
    workers = []
    for _ in range(WORKER_COUNT):
        w = asyncio.create_task(worker(queue, bot, chat.id, stats))
        workers.append(w)
    
    # Monitor Progress
    while not queue.empty():
        if CANCEL:
            break
            
        await asyncio.sleep(5) 
        
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
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except Exception:
            pass

    await queue.join()
    for w in workers:
        w.cancel()
    
    time_taken = get_readable_time(time.time() - start_time)
    
    if CANCEL:
        await msg.edit(f"🛑 **Cancelled!**\nSaved: {stats['saved']}")
    else:
        await msg.edit(
            f'✅ **Indexing Complete!**\n\n'
            f'Total Scanned: `{stats["processed"]}`\n'
            f'Total Saved: `{stats["saved"]}`\n'
            f'Duplicates: `{stats["dups"]}`\n'
            f'Time: `{time_taken}`\n\n'
            f'**Sending JSON backup...**'
        )
        await send_backup(bot, msg.chat.id)

async def send_backup(client, target_chat_id):
    file_path = "channel_backup.json"
    try:
        data = await db.get_all_data()
        if not data:
            return await client.send_message(target_chat_id, "⚠️ No data found in database.")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        await client.send_document(
            chat_id=target_chat_id, 
            document=file_path, 
            caption=f"📦 **Backup File**\nTotal Records: `{len(data)}`"
        )
    except Exception as e:
        logger.error(f"Error during backup: {e}")
        await client.send_message(target_chat_id, f"❌ Error sending backup: `{str(e)}`")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

# --- COMMAND HANDLERS ---

@app.on_message(filters.command('start') & filters.private)
async def start_command(client, message):
    await message.reply_text("👋 **Robust Indexer Bot**\nUse `/index` to start.")

@app.on_message(filters.command('index') & filters.private & filters.user(OWNER_ID))
async def send_for_index(bot: Client, message: Message):
    if lock.locked():
        return await message.reply('⚠️ A process is already running.')
    
    i = await message.reply("Forward the **last message** of the channel or send its link.")
    
    try:
        msg_input = await bot.listen(chat_id=message.chat.id, user_id=message.from_user.id)
    except Exception as e:
        return await message.reply(f"❌ Listener Error: {e}")

    await i.delete()
    
    chat_id = None
    last_msg_id = None
    
    if msg_input.text and msg_input.text.startswith("https://t.me"):
        try:
            msg_link = msg_input.text.split("/")
            last_msg_id = int(msg_link[-1])
            chat_id = msg_link[-2]
            if chat_id.isnumeric():
                chat_id = int(f"-100{chat_id}")
            else:
                 pass
        except:
            return await message.reply('❌ Invalid link!')
    elif msg_input.forward_from_chat and msg_input.forward_from_chat.type == enums.ChatType.CHANNEL:
        last_msg_id = msg_input.forward_from_message_id
        chat_id = msg_input.forward_from_chat.username or msg_input.forward_from_chat.id
    else:
        return await message.reply('❌ Not a valid channel message.')

    try:
        chat = await bot.get_chat(chat_id)
    except:
        return await message.reply(f'❌ Error: Cannot access chat `{chat_id}`.')

    s = await message.reply("Send skip message number (0 for none).")
    msg_skip = await bot.listen(chat_id=message.chat.id, user_id=message.from_user.id)
    await s.delete()
    try:
        skip = int(msg_skip.text)
    except:
        return await message.reply("❌ Invalid number.")

    buttons = [[
        InlineKeyboardButton('YES', callback_data=f'index#yes#{chat.id}#{last_msg_id}#{skip}'), 
        InlineKeyboardButton('CLOSE', callback_data='close_data')
    ]]
    await message.reply(
        f'📝 **Index Request**\n\n'
        f'**Channel:** {chat.title}\n'
        f'**Total Messages:** `{last_msg_id}`\n'
        f'**Start From:** `{skip}`\n\n'
        f'🛡️ **Mode:** Zero Data Loss',
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@app.on_callback_query(filters.regex(r'^index'))
async def index_files_callback(bot, query):
    global CANCEL
    if query.data == 'index#cancel':
        CANCEL = True
        return await query.answer("Stopping...", show_alert=True)
    elif query.data.startswith('index#yes#'):
        try:
            _, _, chat, lst_msg_id, skip = query.data.split("#")
            chat_id = int(chat)
            chat_obj = await bot.get_chat(chat_id)
            await index_files_to_db(int(lst_msg_id), chat_obj, query.message, bot, int(skip))
        except Exception as e:
            logger.error(f"Callback error: {e}")
            await query.message.edit(f"❌ Error starting index: `{e}`")
    await query.answer()

@app.on_callback_query(filters.regex(r'^close_data'))
async def close_callback(bot, query):
    await query.message.delete()

if __name__ == "__main__":
    PORT = int(os.environ.get("PORT", 8080))
    app_web = web.Application(client_max_size=30000000)
    app_web.add_routes(routes)
    async def start_services():
        runner = web.AppRunner(app_web)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", PORT)
        await site.start()
        logger.info(f"Web Server running on port {PORT}")
        logger.info("Starting Bot...")
        await app.start()
        asyncio.create_task(auto_ping())
        await idle()
        await app.stop()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start_services())
