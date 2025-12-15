import os
import json
import asyncio
import logging
import time
from aiohttp import web  # For Render Port Binding
from pyrogram import Client, filters, enums, idle
from pyrogram.errors import FloodWait
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
import pyromod  # <--- CRITICAL: Fixes 'Client object has no attribute listen'

from config import API_ID, API_HASH, BOT_TOKEN, OWNER_ID, INDEX_EXTENSIONS
from db_utils import db

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- WEB SERVER FOR RENDER (AUTO PING) ---
routes = web.RouteTableDef()

@routes.get("/", allow_head=True)
async def root_route_handler(request):
    return web.json_response({"status": "running", "message": "Bot is active"})

async def web_server():
    web_app = web.Application(client_max_size=30000000)
    web_app.add_routes(routes)
    return web_app

# --- UTILS ---
def get_readable_time(seconds: int) -> str:
    count = 0
    up_time = ""
    time_list = []
    time_list.extend([int(seconds / 60 / 60 / 24), int(seconds / 60 / 60) % 24, int(seconds / 60) % 60, int(seconds) % 60])
    for i, j in enumerate(time_list):
        if j != 0:
            up_time += f"{j}{'d' if i==0 else 'h' if i==1 else 'm' if i==2 else 's'} "
    if not up_time:
        return '1s'
    return up_time.strip()

# --- GLOBALS ---
lock = asyncio.Lock()
CANCEL = False 

# --- BOT CLIENT ---
app = Client(
    "BackupChannelBot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# --- CORE INDEXING FUNCTION ---
async def index_files_to_db(lst_msg_id: int, chat, msg: Message, bot: Client, skip: int):
    global CANCEL
    start_time = time.time()
    total_files = 0
    duplicate = 0
    errors = 0
    deleted = 0
    no_media = 0
    unsupported = 0
    current = skip
    
    async with lock:
        try:
            async for message in bot.iter_messages(chat, lst_msg_id, skip):
                time_taken = get_readable_time(time.time() - start_time)
                
                if CANCEL:
                    CANCEL = False
                    await msg.edit(f"Successfully Cancelled!\nCompleted in {time_taken}\n\nSaved {total_files} files!")
                    return

                current += 1
                
                if current % 30 == 0:
                    btn = [[InlineKeyboardButton('CANCEL', callback_data=f'index#cancel')]]
                    try:
                        await msg.edit_text(
                            text=f"Total messages received: <code>{current}</code>\nTotal files saved: <code>{total_files}</code>\nDuplicate: {duplicate} | Errors: {errors}", 
                            reply_markup=InlineKeyboardMarkup(btn)
                        )
                    except FloodWait as e:
                        await asyncio.sleep(e.value)
                    except:
                        pass 

                if message.empty:
                    deleted += 1
                    continue
                elif not message.media:
                    no_media += 1
                    continue
                
                media_type = message.media.value
                if media_type not in [enums.MessageMediaType.VIDEO, enums.MessageMediaType.DOCUMENT]:
                    unsupported += 1
                    continue
                
                media = getattr(message, media_type, None)
                if not media:
                    unsupported += 1
                    continue
                
                file_name = str(media.file_name).lower() if media.file_name else ""
                if not file_name.endswith(INDEX_EXTENSIONS):
                    unsupported += 1
                    continue
                
                sts = await db.save_file(message, media)
                if sts == 'suc':
                    total_files += 1
                elif sts == 'dup':
                    duplicate += 1
                elif sts == 'err':
                    errors += 1

        except Exception as e:
            logger.exception("Indexing loop error")
            await msg.reply(f'Index canceled due to Error - {e}')
        
        # Final Completion
        time_taken = get_readable_time(time.time() - start_time)
        await msg.edit(f'✅ **Indexing Complete!**\nSaved <code>{total_files}</code> files.\nTime: {time_taken}\n\n**Preparing JSON backup...**')
        await send_backup(bot, msg.chat.id)
        
async def send_backup(client, target_chat_id):
    file_path = "channel_backup.json"
    try:
        data = await db.get_all_data()
        if not data:
            return await client.send_message(target_chat_id, "⚠️ No data found in database.")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        await client.send_document(chat_id=target_chat_id, document=file_path, caption=f"📦 **Backup**\nRecords: `{len(data)}`")
    except Exception as e:
        logger.error(f"Error during backup: {e}")
        await client.send_message(target_chat_id, f"❌ Error: `{str(e)}`")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

# --- COMMAND HANDLERS ---

@app.on_message(filters.command('start') & filters.private)
async def start_command(client, message):
    await message.reply_text("👋 **Channel Index & Backup Bot**\nUse `/index` to start.")

@app.on_message(filters.command('index') & filters.private & filters.user(OWNER_ID))
async def send_for_index(bot: Client, message: Message):
    if lock.locked():
        return await message.reply('Wait until previous process complete.')
    
    i = await message.reply("Forward the **last message** of the channel or send its link.")
    
    # This .listen() call will now work because pyromod is imported
    msg_input = await bot.listen(chat_id=message.chat.id, user_id=message.from_user.id)
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
        return await message.reply(f'❌ Error: Cannot access chat. Make sure I am Admin there.')

    s = await message.reply("Send skip message number (0 to start from beginning).")
    msg_skip = await bot.listen(chat_id=message.chat.id, user_id=message.from_user.id)
    await s.delete()
    try:
        skip = int(msg_skip.text)
    except:
        return await message.reply("❌ Invalid number.")

    buttons = [[InlineKeyboardButton('YES', callback_data=f'index#yes#{chat_id}#{last_msg_id}#{skip}'), InlineKeyboardButton('CLOSE', callback_data='close_data')]]
    await message.reply(f'Index **{chat.title}**?\nTotal Messages: <code>{last_msg_id}</code>', reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex(r'^index'))
async def index_files_callback(bot, query):
    global CANCEL
    if query.data == 'index#cancel':
        CANCEL = True
        return await query.answer("Cancelling...", show_alert=True)
    elif query.data.startswith('index#yes#'):
        try:
            _, _, chat, lst_msg_id, skip = query.data.split("#")
            chat = int(chat) if chat.lstrip('-').isdigit() else chat
            await index_files_to_db(int(lst_msg_id), chat, query.message, bot, int(skip))
        except Exception as e:
            await query.message.edit(f"Error: {e}")
    await query.answer()

@app.on_callback_query(filters.regex(r'^close_data'))
async def close_callback(bot, query):
    await query.message.delete()

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    # Get PORT from Render environment, default to 8080
    PORT = int(os.environ.get("PORT", 8080))
    
    # Initialize the Web App
    app_web = web.Application(client_max_size=30000000)
    app_web.add_routes(routes)

    async def start_services():
        # Start Web Server
        runner = web.AppRunner(app_web)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", PORT)
        await site.start()
        logger.info(f"Web Server running on port {PORT}")

        # Start Bot
        logger.info("Starting Bot...")
        await app.start()
        await idle()
        await app.stop()

    # Run everything
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start_services())
