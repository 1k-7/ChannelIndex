import os
import json
import asyncio
import logging
import time
from typing import Union, Optional, AsyncGenerator
from aiohttp import web, ClientSession 
from pyrogram import Client, filters, enums, idle, types
from pyrogram.errors import FloodWait
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
import pyromod

from config import API_ID, API_HASH, BOT_TOKEN, OWNER_ID, INDEX_EXTENSIONS
from db_utils import db

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- CUSTOM ITER_MESSAGES ---
async def iter_messages(
    self: Client, 
    chat_id: Union[int, str], 
    limit: int, 
    offset: int = 0
) -> Optional[AsyncGenerator["types.Message", None]]:
    current = offset
    while True:
        new_diff = min(200, limit - current)
        if new_diff <= 0:
            return
        
        ids_to_fetch = list(range(current, current + new_diff + 1))
        
        try:
            messages = await self.get_messages(chat_id, ids_to_fetch)
        except Exception as e:
            logger.error(f"Error fetching messages {ids_to_fetch[0]}-{ids_to_fetch[-1]}: {e}")
            current += new_diff
            continue

        if not messages:
            current += new_diff
            continue

        for message in messages:
            if message:
                yield message
        
        current += new_diff + 1

Client.iter_messages = iter_messages

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
    time_list = []
    time_list.extend([int(seconds / 86400), int(seconds / 3600) % 24, int(seconds / 60) % 60, int(seconds) % 60])
    up_time = ""
    for i, j in enumerate(time_list):
        if j != 0:
            up_time += f"{j}{'d' if i==0 else 'h' if i==1 else 'm' if i==2 else 's'} "
    return up_time.strip() if up_time else '1s'

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
            async for message in bot.iter_messages(chat.id, lst_msg_id, skip):
                
                if CANCEL:
                    CANCEL = False
                    time_taken = get_readable_time(time.time() - start_time)
                    await msg.edit(f"🛑 **Cancelled!**\nSaved: {total_files}\nTime: {time_taken}")
                    return

                current = message.id 
                
                # Update status
                if total_files > 0 and total_files % 50 == 0:
                    try:
                        btn = [[InlineKeyboardButton('CANCEL', callback_data=f'index#cancel')]]
                        await msg.edit_text(
                            text=f"**Indexing...**\n"
                                 f"Msg ID: `{current}` / `{lst_msg_id}`\n"
                                 f"Saved: `{total_files}`\n"
                                 f"Errors: `{errors}`", 
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
                
                # --- FIX STARTS HERE ---
                # Check the Enum directly, not the value string yet
                if message.media not in [enums.MessageMediaType.VIDEO, enums.MessageMediaType.DOCUMENT, enums.MessageMediaType.AUDIO]:
                    unsupported += 1
                    continue
                
                # Now get the attribute name string (e.g., "video", "document")
                media_type_str = message.media.value
                media = getattr(message, media_type_str, None)
                
                if not media:
                    unsupported += 1
                    continue
                
                # Extension Check
                file_name = str(getattr(media, "file_name", "")).lower()
                
                # Allow videos/audio even without filenames if they have the right type
                # But if it HAS a filename, check the extension.
                if file_name and not file_name.endswith(INDEX_EXTENSIONS):
                    unsupported += 1
                    continue
                # --- FIX ENDS HERE ---
                
                sts = await db.save_file(message, media)
                if sts == 'suc':
                    total_files += 1
                elif sts == 'dup':
                    duplicate += 1
                elif sts == 'err':
                    errors += 1

        except Exception as e:
            logger.exception("Indexing loop error")
            await msg.reply(f'❌ Index canceled due to Error: `{e}`')
        
        time_taken = get_readable_time(time.time() - start_time)
        await msg.edit(
            f'✅ **Indexing Complete!**\n'
            f'Saved: `{total_files}`\n'
            f'Duplicates: `{duplicate}`\n'
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
    await message.reply_text("👋 **Channel Index & Backup Bot**\nUse `/index` to start.")

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
        return await message.reply(f'❌ Error: Cannot access chat `{chat_id}`.\nMake sure I am added as an **Admin** there.')

    s = await message.reply("Send skip message number (Send `0` to start from beginning).")
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
        f'**Start From:** `{skip}`', 
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
