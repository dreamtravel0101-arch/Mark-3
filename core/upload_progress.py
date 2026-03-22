# ============================================================
# UPLOAD PROGRESS TRACKER v1.0
# Tracks successful uploads to prevent re-uploading
# Atomic JSON write • Thread-safe • Backup & recovery
# ============================================================

import json
import threading
import os
from pathlib import Path
from datetime import datetime
from core.utils import BASE_DIR

# ─────────────────────────────────────────────
# CONFIG & LOCK
# ─────────────────────────────────────────────
UPLOAD_PROGRESS_FILE = BASE_DIR / "config/upload_progress.json"
UPLOAD_BACKUP_FILE = BASE_DIR / "config/upload_progress.bak.json"

_lock = threading.RLock()
_cache = None
_cache_hash = None

# ─────────────────────────────────────────────
# NORMALIZE IDS
# ─────────────────────────────────────────────
def _normalize_id(value):
    """Normalize IDs to strings for consistency."""
    if value is None:
        return None
    value = str(value).strip()
    try:
        return str(int(value))
    except Exception:
        return value

# ─────────────────────────────────────────────
# DEFAULT STRUCTURE
# ─────────────────────────────────────────────
def _empty_upload_progress():
    """
    Structure:
    {
      "uploads": {
        "source_channel_id": {
          "target_channel_id": [msg_id_1, msg_id_2, ...],
          ...
        },
        ...
      },
      "upload_history": [
        {
          "source_id": 123,
          "target_id": 456,
          "msg_id": 789,
          "timestamp": "2026-03-18 12:00:00",
          "status": "success"
        },
        ...
      ],
      "meta": {
        "version": 1,
        "last_updated": "2026-03-18 12:00:00"
      }
    }
    """
    return {
        "uploads": {},  # source_id -> {target_id -> [msg_ids]}
        "upload_history": [],  # Full history of uploads
        "meta": {
            "version": 1,
            "last_updated": datetime.now().isoformat(),
        },
    }

# ─────────────────────────────────────────────
# STRUCTURE VALIDATION
# ─────────────────────────────────────────────
def _repair_structure(data):
    """Validate and repair upload progress structure."""
    base = _empty_upload_progress()
    if not isinstance(data, dict):
        return base

    if "uploads" not in data or not isinstance(data["uploads"], dict):
        data["uploads"] = {}
    if "upload_history" not in data or not isinstance(data["upload_history"], list):
        data["upload_history"] = []
    if "meta" not in data or not isinstance(data["meta"], dict):
        data["meta"] = base["meta"]

    return data

# ─────────────────────────────────────────────
# HASH CALCULATION
# ─────────────────────────────────────────────
def _hash_data(data):
    """Calculate stable hash of data."""
    try:
        return hash(json.dumps(data, sort_keys=True, separators=(",", ":")))
    except Exception:
        return None

# ─────────────────────────────────────────────
# LOAD / SAVE
# ─────────────────────────────────────────────
def load_upload_progress(force_reload=False):
    """Load upload progress from JSON file."""
    global _cache, _cache_hash
    with _lock:
        if _cache is not None and not force_reload:
            return _cache

        if not UPLOAD_PROGRESS_FILE.exists():
            UPLOAD_PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
            _cache = _empty_upload_progress()
            save_upload_progress(_cache)
            return _cache

        try:
            with UPLOAD_PROGRESS_FILE.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            print(f"⚠️ upload_progress.json corrupted ({exc}) — attempting recovery...")
            _attempt_recovery()
            try:
                with UPLOAD_PROGRESS_FILE.open("r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as exc2:
                print(f"⚠️ Recovery failed: {exc2}. Starting with empty upload progress.")
                data = _empty_upload_progress()

        repaired = _repair_structure(data)
        _cache = repaired
        _cache_hash = _hash_data(_cache)
        return _cache

def save_upload_progress(data):
    """Save upload progress to JSON file (atomic write)."""
    global _cache, _cache_hash
    with _lock:
        data = _repair_structure(data)
        new_hash = _hash_data(data)
        if _cache_hash is not None and new_hash == _cache_hash:
            return

        UPLOAD_PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp_file = UPLOAD_PROGRESS_FILE.with_suffix(".tmp")

        try:
            with tmp_file.open("w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())

            os.replace(str(tmp_file), str(UPLOAD_PROGRESS_FILE))
        except Exception as e:
            print(f"⚠️ Failed writing upload_progress.json: {e}")
            return

        _cache = json.loads(json.dumps(data))
        _cache_hash = new_hash

# ─────────────────────────────────────────────
# RECOVERY
# ─────────────────────────────────────────────
def _attempt_recovery():
    """Backup current file if possible."""
    try:
        if UPLOAD_PROGRESS_FILE.exists():
            import shutil
            shutil.copy(UPLOAD_PROGRESS_FILE, UPLOAD_BACKUP_FILE)
            print(f"🛟 Backup saved → {UPLOAD_BACKUP_FILE}")
    except Exception:
        pass

# ─────────────────────────────────────────────
# API: RECORD SUCCESSFUL UPLOAD
# ─────────────────────────────────────────────
def mark_uploaded(source_id, target_id, msg_id, telegram_msg_id=None):
    """
    Record a successful upload.
    
    Args:
        source_id: Source channel ID
        target_id: Target channel ID
        msg_id: Original message ID from source
        telegram_msg_id: The ID of uploaded message on Telegram (optional)
    """
    data = load_upload_progress()
    
    source_id = _normalize_id(source_id)
    target_id = _normalize_id(target_id)
    msg_id = _normalize_id(msg_id)
    
    # Add to uploads dict (quick lookup)
    if source_id not in data["uploads"]:
        data["uploads"][source_id] = {}
    if target_id not in data["uploads"][source_id]:
        data["uploads"][source_id][target_id] = []
    
    if msg_id not in data["uploads"][source_id][target_id]:
        data["uploads"][source_id][target_id].append(msg_id)
    
    # Add to history
    history_entry = {
        "source_id": source_id,
        "target_id": target_id,
        "msg_id": msg_id,
        "telegram_msg_id": telegram_msg_id,
        "timestamp": datetime.now().isoformat(),
        "status": "success"
    }
    data["upload_history"].append(history_entry)
    
    # Update timestamp
    data["meta"]["last_updated"] = datetime.now().isoformat()
    
    save_upload_progress(data)
    print(f"✅ Upload recorded: source={source_id}, target={target_id}, msg={msg_id}")

# ─────────────────────────────────────────────
# API: MARK UPLOAD FAILED
# ─────────────────────────────────────────────
def mark_upload_failed(source_id, target_id, msg_id, reason=""):
    """Record a failed upload."""
    data = load_upload_progress()
    
    source_id = _normalize_id(source_id)
    target_id = _normalize_id(target_id)
    msg_id = _normalize_id(msg_id)
    
    # Add to history
    history_entry = {
        "source_id": source_id,
        "target_id": target_id,
        "msg_id": msg_id,
        "timestamp": datetime.now().isoformat(),
        "status": "failed",
        "reason": reason
    }
    data["upload_history"].append(history_entry)
    data["meta"]["last_updated"] = datetime.now().isoformat()
    
    save_upload_progress(data)
    print(f"❌ Upload failed: source={source_id}, target={target_id}, msg={msg_id} - {reason}")

# ─────────────────────────────────────────────
# API: CHECK IF ALREADY UPLOADED
# ─────────────────────────────────────────────
def is_uploaded(source_id, target_id, msg_id):
    """
    Check if a message has already been uploaded to target.
    
    Args:
        source_id: Source channel ID
        target_id: Target channel ID
        msg_id: Message ID
    
    Returns:
        True if already uploaded, False otherwise
    """
    data = load_upload_progress()
    
    source_id = _normalize_id(source_id)
    target_id = _normalize_id(target_id)
    msg_id = _normalize_id(msg_id)
    
    uploads = data.get("uploads", {})
    if source_id in uploads:
        if target_id in uploads[source_id]:
            return msg_id in uploads[source_id][target_id]
    
    return False

# ─────────────────────────────────────────────
# API: GET UPLOADED COUNT
# ─────────────────────────────────────────────
def get_uploaded_count(source_id=None, target_id=None):
    """
    Get count of uploaded messages.
    
    Args:
        source_id: Filter by source (optional)
        target_id: Filter by target (optional)
    
    Returns:
        Count of uploaded messages
    """
    data = load_upload_progress()
    
    if source_id is None and target_id is None:
        # Total uploads
        total = sum(
            len(msgs) for src in data.get("uploads", {}).values()
            for msgs in src.values()
        )
        return total
    
    if source_id is not None:
        source_id = _normalize_id(source_id)
        if source_id in data.get("uploads", {}):
            if target_id is None:
                # Total for this source
                return sum(len(msgs) for msgs in data["uploads"][source_id].values())
            else:
                target_id = _normalize_id(target_id)
                if target_id in data["uploads"][source_id]:
                    return len(data["uploads"][source_id][target_id])
    
    return 0

# ─────────────────────────────────────────────
# API: GET UPLOAD HISTORY
# ─────────────────────────────────────────────
def get_upload_history(source_id=None, target_id=None, status=None, limit=None):
    """
    Get upload history entries.
    
    Args:
        source_id: Filter by source (optional)
        target_id: Filter by target (optional)
        status: Filter by status ('success', 'failed') (optional)
        limit: Limit number of results (optional)
    
    Returns:
        List of history entries
    """
    data = load_upload_progress()
    history = data.get("upload_history", [])
    
    # Apply filters
    filtered = history
    if source_id is not None:
        source_id = _normalize_id(source_id)
        filtered = [h for h in filtered if h.get("source_id") == source_id]
    
    if target_id is not None:
        target_id = _normalize_id(target_id)
        filtered = [h for h in filtered if h.get("target_id") == target_id]
    
    if status is not None:
        filtered = [h for h in filtered if h.get("status") == status]
    
    # Sort by timestamp descending (newest first)
    filtered.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    
    if limit is not None:
        filtered = filtered[:limit]
    
    return filtered

# ─────────────────────────────────────────────
# API: CLEAR UPLOADS
# ─────────────────────────────────────────────
def clear_uploads(source_id=None, target_id=None):
    """
    Clear upload records.
    
    Args:
        source_id: Clear only this source (optional)
        target_id: Clear only this target (optional)
    """
    data = load_upload_progress()
    
    if source_id is None and target_id is None:
        data["uploads"] = {}
        print("✅ All uploads cleared")
    elif source_id is not None:
        source_id = _normalize_id(source_id)
        if source_id in data["uploads"]:
            if target_id is None:
                del data["uploads"][source_id]
                print(f"✅ Uploads cleared for source {source_id}")
            else:
                target_id = _normalize_id(target_id)
                if target_id in data["uploads"][source_id]:
                    del data["uploads"][source_id][target_id]
                    print(f"✅ Uploads cleared for source {source_id} → target {target_id}")
    
    data["meta"]["last_updated"] = datetime.now().isoformat()
    save_upload_progress(data)

# ─────────────────────────────────────────────
# API: GET SUMMARY
# ─────────────────────────────────────────────
def get_upload_summary():
    """Get summary statistics."""
    data = load_upload_progress()
    
    total_uploads = sum(
        len(msgs) for src in data.get("uploads", {}).values()
        for msgs in src.values()
    )
    
    history = data.get("upload_history", [])
    successful = sum(1 for h in history if h.get("status") == "success")
    failed = sum(1 for h in history if h.get("status") == "failed")
    
    return {
        "total_upload_records": total_uploads,
        "successful_uploads": successful,
        "failed_uploads": failed,
        "total_history_entries": len(history),
        "last_updated": data.get("meta", {}).get("last_updated", "Never")
    }
