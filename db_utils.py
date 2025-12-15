# db_utils.py

import logging
import re
import base64
from struct import pack
from pyrogram.file_id import FileId, FileType
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError

from config import MONGO_URI, DATABASE_NAME, COLLECTION_NAME

logger = logging.getLogger(__name__)

# --- Custom File ID Encoding (from ia_filterdb.py) ---

def encode_file_id(s: bytes) -> str:
    """Encodes the packed file ID bytes into a URL-safe base64 string."""
    r = b""
    n = 0
    for i in s + bytes([22]) + bytes([4]):
        if i == 0:
            n += 1
        else:
            if n:
                r += b"\x00" + bytes([n])
                n = 0
            r += bytes([i])
    return base64.urlsafe_b64encode(r).decode().rstrip("=")

def unpack_new_file_id(new_file_id: str) -> str:
    """
    Decodes a standard Telegram file_id into the custom-encoded file_id 
    used as the MongoDB _id. (Logic from ia_filterdb.py)
    """
    try:
        decoded = FileId.decode(new_file_id)
    except:
        # Fallback if FileId.decode fails for some file types
        return new_file_id 

    file_id_encoded = encode_file_id(
        pack(
            "<iiqq",
            int(decoded.file_type),
            decoded.dc_id,
            decoded.media_id,
            decoded.access_hash
        )
    )
    return file_id_encoded

# --- Database Class ---

class Database:
    def __init__(self):
        self._client = AsyncIOMotorClient(MONGO_URI)
        self.db = self._client[DATABASE_NAME]
        self.col = self.db[COLLECTION_NAME]

    async def save_file(self, message, media):
        """
        Save file in database, extracting all required metadata.
        
        New fields added:
        - file_id_tg: The original Telegram File ID (for easy use)
        - message_id: The Telegram message ID
        """
        
        # 1. Custom File ID for MongoDB _id (from ia_filterdb.py)
        file_id_encoded = unpack_new_file_id(media.file_id)
        
        # 2. Clean file name and caption (from ia_filterdb.py)
        file_name = re.sub(r"@\w+|(_|\-|\.|\+)", " ", str(media.file_name or media.file_unique_id))
        file_caption = re.sub(r"@\w+|(_|\-|\.|\+)", " ", str(message.caption or ""))
        
        document = {
            '_id': file_id_encoded,
            'file_name': file_name.strip(),
            'file_size': media.file_size,
            'file_id_tg': media.file_id,
            'file_unique_id': media.file_unique_id, # Added for completeness
            'message_id': message.id,
            'chat_id': message.chat.id,
            'caption': file_caption.strip()
        }
        
        try:
            await self.col.insert_one(document)
            logger.info(f'Saved - {file_name}')
            return 'suc'
        except DuplicateKeyError:
            logger.warning(f'Already Saved - {file_name}')
            return 'dup'
        except Exception as e:
            logger.error(f'Error saving file {file_name}: {e}')
            return 'err'

    async def get_all_data(self):
        """Retrieves all documents from the collection for backup."""
        # Use to_list(length=None) to get all documents
        cursor = self.col.find({})
        data = await cursor.to_list(length=None)
        
        # Remove MongoDB '_id' object for JSON serialization
        for item in data:
            if '_id' in item:
                del item['_id']
        return data

# Initialize the database instance
db = Database()