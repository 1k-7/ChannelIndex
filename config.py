# config.py

# --- BOT CONFIGURATION ---
API_ID = 24574155               # Your Telegram API ID
API_HASH = "94001be339d1264432c215f698bc3868"     # Your Telegram API Hash
BOT_TOKEN = "8258848736:AAFfDwmgNrtuaC0k7x9DZdKECYs3SLR4HgE"   # Your Bot Token
OWNER_ID = 7679063455           # Your Telegram User ID (for command restriction)

# --- MONGODB CONFIGURATION ---
MONGO_URI = "mongodb+srv://Chin:vq0AtXopWzmAJItl@cluster0.vcuudlg.mongodb.net/?appName=Cluster0"
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