import os
import json
import asyncio
import logging
import time
from typing import Union, Optional, List
from aiohttp import web, ClientSession 
from pyrogram import Client, filters, enums, idle, types
from pyrogram.errors import FloodWait
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
import pyromod

from config import API_ID, API_HASH, BOT_TOKEN, OWNER_ID, INDEX_EXTENSIONS
from db_utils import db, unpack_new_file_id

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
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
    logger.info(f"Auto-ping started for URL: {url}")
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
BATCH_SIZE = 200 # Telegram API Limit per call
CONCURRENCY_LIMIT = 5 # Number of parallel batches (Adjust carefully to avoid flood)

# --- BOT CLIENT ---
app = Client(
    "BackupChannelBot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# --- FAST INDEXING LOGIC ---

async def process_batch(bot, chat_id, ids, semaphore):
    """Fetches a batch of messages and returns valid file documents."""
    valid_docs = []
    
    async with semaphore:
        try:
            # Fetch 200 messages at once
            messages = await bot.get_messages(chat_id, ids)
        except FloodWait as e:
            logger.warning(f"FloodWait of {e.value}s encountered. Sleeping...")
            await asyncio.sleep(e.value + 1)
            return await process_batch(bot, chat_id, ids, semaphore) # Retry
        except Exception as e:
            logger.error(f"Error fetching batch {ids[0]}-{ids[-1]}: {e}")
            return []

    if not messages:
        return []

    for message in messages:
        if not message or message.empty or not message.media:
            continue
        
        # Media Type Check
        if message.media not in [enums.MessageMediaType.VIDEO, enums.MessageMediaType.DOCUMENT, enums.MessageMediaType.AUDIO]:
            continue
        
        media_type_str = message.media.value
        media = getattr(message, media_type_str, None)
        if not media:
            continue
            
        file_name = str(getattr(media, "file_name", "")).lower()
        
        # Extension Check (Skip if strict extension check fails, unless it's a video without name)
        if file_name and not file_name.endswith(INDEX_EXTENSIONS):
            continue

        # Prepare Document for Bulk Insert
        try:
            file_id_encoded = unpack_new_file_id(media.file_id)
            caption = message.caption or ""
            # Basic cleaning similar to your original regex
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
        except Exception as e:
            logger.error(f"Error parsing message {message.id}: {e}")
            continue
            
    return valid_docs

async def index_files_to_db(last_msg_id: int, chat, msg: Message, bot: Client, start_from: int):
    global CANCEL
    start_time = time.time()
    
    total_processed = 0
    total_saved = 0
    total_duplicates = 0
    
    # Generate list of batch ranges
    # e.g., range(1, 1000) -> [1...200], [201...400]...
    all_ids = list(range(start_from + 1, last_msg_id + 1))
    chunks = [all_ids[i:i + BATCH_SIZE] for i in range(0, len(all_ids), BATCH_SIZE)]
    
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    tasks = set()
    
    await msg.edit(f"🚀 **Fast Indexing Started!**\n\nTotal Messages: `{len(all_ids)}`\nBatches: `{len(chunks)}`\nConcurrent Workers: `{CONCURRENCY_LIMIT}`")
    
    # Iterate through chunks and spawn tasks
    for i, chunk_ids in enumerate(chunks):
        if CANCEL:
            break
            
        task = asyncio.create_task(process_batch(bot, chat.id, chunk_ids, semaphore))
        tasks.add(task)
        
        # Collect results as they finish to save memory and update status
        # We wait if we have too many active tasks, or just let them run?
        # Better approach: Add to set, and periodically gather finished ones.
        
        # To keep it simple and ensure we don't spawn 10,000 tasks instantly:
        if len(tasks) >= CONCURRENCY_LIMIT * 2:
            # Wait for at least one task to finish
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            
            for t in done:
                docs = await t
                if docs:
                    saved, dups = await db.save_batch(docs)
                    total_saved += saved
                    total_duplicates += dups
                total_processed += BATCH_SIZE

            # Update status
            if i % 5 == 0:
                time_taken = get_readable_time(time.time() - start_time)
                try:
                    btn = [[InlineKeyboardButton('CANCEL', callback_data=f'index#cancel')]]
                    await msg.edit_text(
                        f"⚡ **Fast Indexing...**\n\n"
                        f"Processed: `{total_processed}` / `{last_msg_id}`\n"
                        f"Saved: `{total_saved}`\n"
                        f"Duplicates: `{total_duplicates}`\n"
                        f"Time: `{time_taken}`",
                        reply_markup=InlineKeyboardMarkup(btn)
                    )
                except FloodWait as e:
                    await asyncio.sleep(e.value)
                except:
                    pass

    # Process remaining tasks
    if tasks:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
        for t in done:
            docs = await t
            if docs:
                saved, dups = await db.save_batch(docs)
                total_saved += saved
                total_duplicates += dups

    # Final Message
    if CANCEL:
        await msg.edit(f"🛑 **Cancelled!**\nSaved: {total_saved}")
    else:
        time_taken = get_readable_time(time.time() - start_time)
        await msg.edit(
            f'✅ **Indexing Complete!**\n\n'
            f'Total Saved: `{total_saved}`\n'
            f'Duplicates: `{total_duplicates}`\n'
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
    await message.reply_text("👋 **Fast Indexer Bot**\nUse `/index` to start.")

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
        f'🚀 **Mode:** Fast Parallel Index',
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@app.on_callback_query(filters.regex(r'^index'))
async def index_files_callback(bot, query):
    global CANCEL
    if query.data == 'index#cancel':
        CANCEL = True
        return await query.answer("Cancelling...", show_alert=True)
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
