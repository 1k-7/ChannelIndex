# config.py

# --- BOT CONFIGURATION ---
API_ID = 1234567               # Your Telegram API ID
API_HASH = "YOUR_API_HASH"     # Your Telegram API Hash
BOT_TOKEN = "YOUR_BOT_TOKEN"   # Your Bot Token
OWNER_ID = 123456789           # Your Telegram User ID (for command restriction)

# --- MONGODB CONFIGURATION ---
MONGO_URI = "mongodb+srv://user:password@clustername.mongodb.net/?retryWrites=true&w=majority"
DATABASE_NAME = "ChannelBackupDB"
COLLECTION_NAME = "channel_backup_files"

# --- INDEXING FILTERS ---
# These extensions match typical indexable media types.
# You can customize this list.
INDEX_EXTENSIONS = (
    ".mkv", ".mp4", ".mov", ".webm", ".ts", ".flv", ".avi", ".m4v",
    ".zip", ".rar", ".7z", ".tar", ".gz", ".iso", ".apk", ".exe",
    ".pdf", ".epub", ".mobi", ".doc", ".docx", ".xls", ".xlsx",
    ".srt", ".ass", ".txt",
    ".mp3", ".flac", ".ogg", ".wav",
)