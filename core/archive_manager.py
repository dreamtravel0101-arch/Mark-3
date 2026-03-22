import json
import base64
from datetime import datetime
from pathlib import Path
from threading import RLock

from core.utils import BASE_DIR

ARCHIVE_DIR = BASE_DIR / "config" / "archives"
_lock = RLock()


def _json_serializer(obj):
    """Custom JSON serializer for objects not serializable by default json code."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, bytes):
        # Convert bytes to base64 string for JSON serialization
        return {
            "__bytes__": True,
            "data": base64.b64encode(obj).decode('ascii')
        }
    raise TypeError(f"Type {type(obj)} not serializable")


def _deserialize_bytes(obj):
    """Recursively deserialize base64-encoded bytes back to bytes objects."""
    if isinstance(obj, dict):
        if obj.get("__bytes__") and "data" in obj:
            try:
                return base64.b64decode(obj["data"])
            except Exception:
                return obj
        return {k: _deserialize_bytes(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_deserialize_bytes(item) for item in obj]
    return obj


def _archive_file(chat_id: int) -> Path:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    return ARCHIVE_DIR / f"{chat_id}.json"


def load_archive(chat_id: int) -> dict:
    # use safe json reading to avoid "bad unicode escape" errors etc.
    try:
        path = _archive_file(chat_id)
        from core.utils import read_json
        data = read_json(path, {})
        if isinstance(data, dict):
            # Deserialize base64-encoded bytes back to bytes objects
            return _deserialize_bytes(data)
    except Exception:
        pass
    return {}


def save_archive(chat_id: int, data: dict):
    path = _archive_file(chat_id)
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=_json_serializer)
    except Exception as e:
        print(f"[Archive] failed to save archive for {chat_id}: {e}")


def record_message(msg):
    """Store the given :class:`telethon.tl.types.Message`.

    The message is serialised using ``message.to_dict()`` so that it can be
    re-created later even if the original object has been garbage-collected.
    """
    try:
        chat_id = msg.chat_id or getattr(msg.to_id, "channel_id", None)
        if chat_id is None:
            return
        with _lock:
            archive = load_archive(chat_id)
            archive[str(msg.id)] = msg.to_dict()
            save_archive(chat_id, archive)
    except Exception as e:
        if hasattr(msg, "id"):
            print(f"[Archive] error recording message {msg.id}: {e}")


def get_message(chat_id: int, msg_id: int):
    archive = load_archive(chat_id)
    return archive.get(str(msg_id))


# helper for scanning messages on start/periodically
async def rebuild_from_history(client, chat, limit=1000):
    """Fetch recent messages and ensure they are in the archive.
    Useful for backfilling when the bot was offline.
    """
    async for msg in client.iter_messages(chat, limit=limit):
        record_message(msg)


# ─────────────────────────────────────────────
# ENHANCED BACKUP & RECOVERY SYSTEM
# ─────────────────────────────────────────────

def archive_message_during_relay(msg, source_key):
    """Archive message during relay operations for guaranteed recovery."""
    try:
        chat_id = msg.chat_id or getattr(msg.to_id, "channel_id", None)
        if chat_id is None:
            return

        with _lock:
            archive = load_archive(chat_id)
            msg_data = msg.to_dict()

            # Add relay metadata
            msg_data["_relay_backup"] = {
                "source_key": source_key,
                "archived_at": str(Path(__file__).stat().st_mtime),  # timestamp
                "has_media": bool(getattr(msg, 'media', None)),
                "has_text": bool(getattr(msg, 'text', None) or getattr(msg, 'caption', None)),
            }

            archive[str(msg.id)] = msg_data
            save_archive(chat_id, archive)

            print(f"[Archive] 📦 Backed up message {msg.id} from {source_key}")

    except Exception as e:
        print(f"[Archive] ⚠️ Failed to backup message {getattr(msg, 'id', 'unknown')}: {e}")


def recover_from_archive(chat_id: int, msg_id: int):
    """Recover a message from archive if admin log fails."""
    try:
        archive = load_archive(chat_id)
        msg_data = archive.get(str(msg_id))

        if not msg_data:
            return None

        # Check if this was a relay backup
        if "_relay_backup" not in msg_data:
            return None

        print(f"[Archive] 🔄 Recovered message {msg_id} from backup")

        # Convert back to a minimal message-like object
        class RecoveredMessage:
            def __init__(self, data):
                self.id = int(data.get("id", 0))
                self.chat_id = chat_id
                self.text = data.get("message", "")
                self.caption = data.get("caption", "")
                self.media = data.get("media")
                self.date = data.get("date")
                self.forward = data.get("fwd_from") or data.get("forward")
                self.entities = data.get("entities", [])
                self.reply_to = data.get("reply_to")

                # Mark as recovered
                self._recovered_from_archive = True
                self._backup_info = data.get("_relay_backup", {})

        return RecoveredMessage(msg_data)

    except Exception as e:
        print(f"[Archive] ⚠️ Failed to recover message {msg_id}: {e}")
        return None


def get_archived_message_count(chat_id: int) -> int:
    """Get total number of archived messages for a chat."""
    archive = load_archive(chat_id)
    return len(archive)


def cleanup_old_archives(max_age_days=30):
    """Clean up archives older than specified days to save space."""
    import time
    import os

    try:
        cutoff = time.time() - (max_age_days * 24 * 60 * 60)

        for archive_file in ARCHIVE_DIR.glob("*.json"):
            if archive_file.stat().st_mtime < cutoff:
                try:
                    archive_file.unlink()
                    print(f"[Archive] 🗑️ Cleaned up old archive: {archive_file.name}")
                except Exception as e:
                    print(f"[Archive] ⚠️ Failed to clean up {archive_file.name}: {e}")

    except Exception as e:
        print(f"[Archive] ⚠️ Archive cleanup failed: {e}")


def list_archived_chats():
    """List all chats that have archived messages."""
    try:
        return [int(f.stem) for f in ARCHIVE_DIR.glob("*.json") if f.is_file()]
    except Exception:
        return []
