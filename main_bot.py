# main_bot.py

import os
import json
import asyncio
import logging
import time
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

from config import API_ID, API_HASH, BOT_TOKEN, OWNER_ID, INDEX_EXTENSIONS
from db_utils import db

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- UTILS (Simplified from repository's utils.py) ---

def get_readable_time(seconds: int) -> str:
    """Converts seconds into a readable format (D:H:M:S)"""
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
CANCEL = False # Used to signal cancellation, similar to temp.CANCEL in index.py

# --- BOT CLIENT ---
app = Client(
    "BackupChannelBot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# --- CORE INDEXING FUNCTION (Adapted from index.py) ---

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
    
    # Use lock to prevent concurrent indexing
    async with lock:
        try:
            # Iterates messages from the last ID down to the first, skipping the specified number
            async for message in bot.iter_messages(chat, lst_msg_id, skip):
                
                time_taken = get_readable_time(time.time() - start_time)
                
                # Handle Cancellation check
                if CANCEL:
                    CANCEL = False
                    await msg.edit(
                        f"Successfully Cancelled!\nCompleted in {time_taken}\n\n"
                        f"Saved <code>{total_files}</code> files to Database!\n"
                        f"Duplicate Files Skipped: <code>{duplicate}</code>\n"
                        f"Deleted Messages Skipped: <code>{deleted}</code>\n"
                        f"Non-Media messages skipped: <code>{no_media + unsupported}</code>\n"
                        f"Errors Occurred: <code>{errors}</code>"
                    )
                    return

                current += 1
                
                # Update status message every 30 messages
                if current % 30 == 0:
                    btn = [[
                        InlineKeyboardButton('CANCEL', callback_data=f'index#cancel')
                    ]]
                    try:
                        await msg.edit_text(
                            text=f"Total messages received: <code>{current}</code>\n"
                                 f"Total files saved: <code>{total_files}</code>\n"
                                 f"Duplicate Files Skipped: <code>{duplicate}</code>\n"
                                 f"Deleted Messages Skipped: <code>{deleted}</code>\n"
                                 f"Non-Media messages skipped: <code>{no_media + unsupported}</code>\n"
                                 f"Errors Occurred: <code>{errors}</code>", 
                            reply_markup=InlineKeyboardMarkup(btn)
                        )
                    except FloodWait as e:
                        await asyncio.sleep(e.value)
                    except:
                        pass # Ignore other minor editing errors

                if message.empty:
                    deleted += 1
                    continue
                elif not message.media:
                    no_media += 1
                    continue
                
                # Filter by media type (VIDEO or DOCUMENT, matching index.py)
                media_type = message.media.value
                if media_type not in [enums.MessageMediaType.VIDEO, enums.MessageMediaType.DOCUMENT]:
                    unsupported += 1
                    continue
                
                # Get the media object (video or document)
                media = getattr(message, media_type, None)
                if not media:
                    unsupported += 1
                    continue
                
                # Filter by file extension
                file_name = str(media.file_name).lower() if media.file_name else ""
                if not file_name.endswith(INDEX_EXTENSIONS):
                    unsupported += 1
                    continue
                
                # Pass the full message object and the media object to save_file
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
        
        # --- Final Completion ---
        
        time_taken = get_readable_time(time.time() - start_time)
        
        # 1. Update final status message
        await msg.edit(
            f'✅ **Indexing Complete!**\n'
            f'Successfully saved <code>{total_files}</code> files to Database!\n'
            f'Completed in {time_taken}\n\n'
            f'Duplicate Files Skipped: <code>{duplicate}</code>\n'
            f'Deleted Messages Skipped: <code>{deleted}</code>\n'
            f'Non-Media messages skipped: <code>{no_media + unsupported}</code>\n'
            f'Errors Occurred: <code>{errors}</code>\n\n'
            f'**Preparing JSON backup...**'
        )
        
        # 2. Send the JSON backup
        await send_backup(bot, msg.chat.id)
        
        
async def send_backup(client, target_chat_id):
    """Fetches data from MongoDB, dumps to JSON, and sends it."""
    file_path = "channel_backup.json"
    
    try:
        data = await db.get_all_data()
        
        if not data:
            return await client.send_message(target_chat_id, "⚠️ No data found in database to backup.")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        await client.send_document(
            chat_id=target_chat_id,
            document=file_path,
            caption=f"📦 **Channel Backup**\nTotal Records: `{len(data)}`"
        )
        logger.info("JSON backup sent successfully.")
        
    except Exception as e:
        logger.error(f"Error during backup process: {e}")
        await client.send_message(target_chat_id, f"❌ **Error generating/sending backup:**\n`{str(e)}`")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

# --- COMMAND HANDLERS ---

@app.on_message(filters.command('start') & filters.private)
async def start_command(client, message):
    await message.reply_text(
        "👋 **Channel Index and Backup Bot**\n\n"
        "I am based on your auto-filter bot's indexing logic.\n"
        "Use `/index` to start the backup process."
    )

@app.on_message(filters.command('index') & filters.private & filters.user(OWNER_ID))
async def send_for_index(bot: Client, message: Message):
    """Initial /index command handler (Adapted from index.py)"""
    if lock.locked():
        return await message.reply('Wait until previous process complete.')
    
    i = await message.reply("Forward the **last message** of the channel or send its link.")
    msg_input = await bot.listen(chat_id=message.chat.id, user_id=message.from_user.id)
    await i.delete()
    
    chat_id = None
    last_msg_id = None
    
    # Logic to parse forwarded message or link
    if msg_input.text and msg_input.text.startswith("https://t.me"):
        try:
            msg_link = msg_input.text.split("/")
            last_msg_id = int(msg_link[-1])
            chat_id = msg_link[-2]
            if chat_id.isnumeric():
                # Convert public channel ID to internal format -100xxxxxxxxxx
                chat_id = int(f"-100{chat_id}")
        except:
            return await message.reply('❌ Invalid message link format!')
    elif msg_input.forward_from_chat and msg_input.forward_from_chat.type == enums.ChatType.CHANNEL:
        last_msg_id = msg_input.forward_from_message_id
        chat_id = msg_input.forward_from_chat.username or msg_input.forward_from_chat.id
    else:
        return await message.reply('❌ This is not a forwarded message or a valid channel link.')

    try:
        chat = await bot.get_chat(chat_id)
    except Exception as e:
        return await message.reply(f'❌ **Error fetching chat:**\n`{e}`')

    if chat.type != enums.ChatType.CHANNEL:
        return await message.reply("❌ I can index only channels.")

    s = await message.reply("Send skip message number (0 to start from the beginning).")
    msg_skip = await bot.listen(chat_id=message.chat.id, user_id=message.from_user.id)
    await s.delete()
    try:
        skip = int(msg_skip.text)
    except:
        return await message.reply("❌ Skip number is invalid.")

    # Confirmation step
    buttons = [[
        InlineKeyboardButton('YES', callback_data=f'index#yes#{chat_id}#{last_msg_id}#{skip}')
    ],[
        InlineKeyboardButton('CLOSE', callback_data='close_data'),
    ]]
    reply_markup = InlineKeyboardMarkup(buttons)
    await message.reply(
        f'Do you want to index **{chat.title}** channel?\n'
        f'Total Messages to iterate from: <code>{last_msg_id}</code>\n'
        f'Starting from message: <code>{skip + 1}</code>', 
        reply_markup=reply_markup
    )

@app.on_callback_query(filters.regex(r'^index'))
async def index_files_callback(bot, query):
    """Callback query handler for index confirmation/cancellation"""
    global CANCEL
    
    # Simple check for the 'cancel' button only
    if query.data == 'index#cancel':
        CANCEL = True
        return await query.answer("Cancellation request sent.", show_alert=True)
    elif query.data == 'index#yes#':
        # Parse the full data for starting index
        try:
            _, ident, chat, lst_msg_id, skip = query.data.split("#")
        except ValueError:
             return await query.answer("Invalid callback data.", show_alert=True)
             
        msg = query.message
        await msg.edit("Starting Indexing...")
        
        try:
            chat = int(chat)
        except ValueError:
            pass # Keep as username if not integer

        # Start the main indexing process
        await index_files_to_db(int(lst_msg_id), chat, msg, bot, int(skip))
    
    await query.answer()

@app.on_callback_query(filters.regex(r'^close_data'))
async def close_callback(bot, query):
    await query.message.delete()
    await query.answer()

if __name__ == "__main__":
    logger.info("Starting Bot...")
    app.run()