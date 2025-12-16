import logging
import re
import base64
from struct import pack
from pyrogram.file_id import FileId
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import BulkWriteError, DuplicateKeyError

from config import MONGO_URI, DATABASE_NAME, COLLECTION_NAME

logger = logging.getLogger(__name__)

# --- Custom File ID Encoding (From ia_filterdb.py) ---
def encode_file_id(s: bytes) -> str:
    r = b""
    n = 0
    for i in s + bytes([22]) + bytes([4]):
        if i == 0: n += 1
        else:
            if n: r += b"\x00" + bytes([n]); n = 0
            r += bytes([i])
    return base64.urlsafe_b64encode(r).decode().rstrip("=")

def unpack_new_file_id(new_file_id: str) -> str:
    try: decoded = FileId.decode(new_file_id)
    except: return new_file_id 
    return encode_file_id(pack("<iiqq", int(decoded.file_type), decoded.dc_id, decoded.media_id, decoded.access_hash))

class Database:
    def __init__(self):
        self._client = AsyncIOMotorClient(MONGO_URI)
        self.db = self._client[DATABASE_NAME]
        self.col = self.db[COLLECTION_NAME]

    async def save_batch(self, batch_data):
        """
        Bulk save using the exact document structure from ia_filterdb.py
        """
        if not batch_data: return 0, 0
        try:
            # ordered=False allows valid docs to insert even if some fail (duplicates)
            result = await self.col.insert_many(batch_data, ordered=False)
            inserted = len(result.inserted_ids)
            duplicates = len(batch_data) - inserted
            return inserted, duplicates
        except BulkWriteError as e:
            inserted = e.details['nInserted']
            duplicates = len(batch_data) - inserted
            return inserted, duplicates
        except Exception as e:
            logger.error(f"DB Batch Error: {e}")
            return 0, 0

    async def get_all_data(self):
        cursor = self.col.find({})
        data = await cursor.to_list(length=None)
        for item in data:
            if '_id' in item: del item['_id']
        return data

    async def drop_collection(self):
        await self.col.drop()

db = Database()
