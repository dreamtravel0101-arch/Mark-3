# ============================================================
# PROGRESS MANAGER v2.2 (ULTRA SAFE + ATOMIC + ROBUST)
# ✅ Full thread-safe JSON handling
# ✅ Auto-backup & recovery
# ✅ Download/skipped/failed tracking
# ✅ Last processed tracking
# ✅ Atomic write + hash check
# ✅ Fully backward compatible
# ============================================================

import json
import threading
import os
import shutil
from pathlib import Path
from core.utils import BASE_DIR

# ─────────────────────────────────────────────
# CONFIG & LOCK
# ─────────────────────────────────────────────
PROGRESS_FILE = BASE_DIR / "config/progress.json"
BACKUP_FILE = BASE_DIR / "config/progress.bak.json"

_lock = threading.RLock()
_cache = None
_cache_hash = None
_download_index = {}  # O(1) lookup index

# ─────────────────────────────────────────────
# ID NORMALIZATION
# ─────────────────────────────────────────────
def _normalize_id(value):
    """Normalize Telegram IDs or composite keys safely."""
    if value is None:
        return None

    value = str(value).strip()
    if "__TO__" in value:
        parts = value.split("__TO__")
        return "__TO__".join(_normalize_id(p) for p in parts)
    try:
        return str(int(value))
    except Exception:
        return value

# ─────────────────────────────────────────────
# DEFAULT STRUCTURE
# ─────────────────────────────────────────────
def _empty_progress():
    return {
        "downloads": {},
        "skipped": {"downloads": [], "uploads": []},
        "failed": {"downloads": [], "uploads": []},
        "last_processed": {},
        "batch_checkpoints": {},  # NEW: Track batch boundaries for resume logic
        "meta": {
            "auto_skip_downloaded": True,
            "version": 11,
        },
    }

# ─────────────────────────────────────────────
# STRUCTURE VALIDATION / REPAIR
# ─────────────────────────────────────────────
def _repair_structure(data):
    base = _empty_progress()
    if not isinstance(data, dict):
        return base

    for key in base:
        if key not in data or not isinstance(data[key], type(base[key])):
            data[key] = base[key]

    for mode in ["skipped", "failed"]:
        if not isinstance(data[mode], dict):
            data[mode] = {"downloads": [], "uploads": []}
        for k in ["downloads", "uploads"]:
            if not isinstance(data[mode].get(k), list):
                data[mode][k] = []

    for key in ["downloads", "last_processed", "meta"]:
        if not isinstance(data[key], dict):
            data[key] = base[key]

    return data

# ─────────────────────────────────────────────
# INDEX BUILDER
# ─────────────────────────────────────────────
def _rebuild_index(data):
    global _download_index
    new_index = {}
    for source_key, msg_list in data.get("downloads", {}).items():
        key = _normalize_id(source_key)
        if isinstance(msg_list, list):
            cleaned = set()
            for x in msg_list:
                try:
                    cleaned.add(int(x))
                except Exception:
                    continue
            new_index[key] = cleaned
        else:
            new_index[key] = set()
    _download_index = new_index

# ─────────────────────────────────────────────
# HASH (STABLE + SAFE)
# ─────────────────────────────────────────────
def _hash_data(data):
    try:
        return hash(json.dumps(data, sort_keys=True, separators=(",", ":")))
    except Exception:
        return None

# ─────────────────────────────────────────────
# LOAD / SAVE (ATOMIC + SAFE)
# ─────────────────────────────────────────────
def load_progress(force_reload=False):
    global _cache, _cache_hash
    with _lock:
        if _cache is not None and not force_reload:
            return _cache

        if not PROGRESS_FILE.exists():
            PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
            _cache = _empty_progress()
            _rebuild_index(_cache)
            save_progress(_cache)
            return _cache

        try:
            with PROGRESS_FILE.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            # log the exact error so the user can fix the file manually
            print(f"⚠️ progress.json corrupted ({exc}) — attempting recovery...")
            _attempt_recovery()
            try:
                with PROGRESS_FILE.open("r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as exc2:
                print(f"⚠️ Recovery failed: {exc2}. Starting with empty progress.")
                data = _empty_progress()

        repaired = _repair_structure(data)
        _cache = repaired
        _rebuild_index(_cache)
        _cache_hash = _hash_data(_cache)
        return _cache

def save_progress(data):
    global _cache, _cache_hash
    with _lock:
        data = _repair_structure(data)
        new_hash = _hash_data(data)
        if _cache_hash is not None and new_hash == _cache_hash:
            return

        PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp_file = PROGRESS_FILE.with_suffix(".tmp")

        try:
            with tmp_file.open("w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())

            # Use os.replace with explicit string paths for Windows reliability
            try:
                os.replace(str(tmp_file), str(PROGRESS_FILE))
            except Exception:
                # Fallback to pathlib replace if os.replace fails for any reason
                try:
                    tmp_file.replace(PROGRESS_FILE)
                except Exception as e:
                    print(f"⚠️ Failed writing progress.json (rename error): {e} | src={tmp_file} dst={PROGRESS_FILE}")
                    return
        except Exception as e:
            print(f"⚠️ Failed writing progress.json: {e} | tmp={tmp_file} dst={PROGRESS_FILE}")
            return

        _cache = json.loads(json.dumps(data))
        _cache_hash = new_hash
        _rebuild_index(_cache)

# ─────────────────────────────────────────────
# RECOVERY
# ─────────────────────────────────────────────
def _attempt_recovery():
    try:
        if PROGRESS_FILE.exists():
            shutil.copy(PROGRESS_FILE, BACKUP_FILE)
            print(f"🛟 Backup saved → {BACKUP_FILE}")
    except Exception:
        pass

# ─────────────────────────────────────────────
# DOWNLOAD TRACKING
# ─────────────────────────────────────────────
def mark_downloaded(msg_id: int, source_key):
    source_key = _normalize_id(source_key)
    try:
        msg_id = int(msg_id)
    except Exception:
        return

    data = load_progress()
    src_list = data["downloads"].setdefault(source_key, [])
    src_set = _download_index.setdefault(source_key, set())

    if msg_id in src_set:
        return

    src_list.append(msg_id)
    src_set.add(msg_id)

    if data["meta"].get("auto_skip_downloaded", True):
        entry = f"{source_key}:{msg_id}"
        skip_list = data["skipped"].setdefault("downloads", [])
        if entry not in skip_list:
            skip_list.append(entry)

    save_progress(data)

def is_downloaded(msg_id: int, source_key) -> bool:
    source_key = _normalize_id(source_key)
    try:
        msg_id = int(msg_id)
    except Exception:
        return False

    load_progress()
    return msg_id in _download_index.get(source_key, set())

# ─────────────────────────────────────────────
# SKIPPED / FAILED
# ─────────────────────────────────────────────
def mark_skipped(entry, mode="downloads"):
    data = load_progress()
    lst = data["skipped"].setdefault(mode, [])
    if entry not in lst:
        lst.append(entry)
        save_progress(data)

def mark_failed(entry, mode="downloads"):
    data = load_progress()
    lst = data["failed"].setdefault(mode, [])
    if entry not in lst:
        lst.append(entry)
        save_progress(data)

# ─────────────────────────────────────────────
# LAST PROCESSED
# ─────────────────────────────────────────────
def _make_key(source_id, target_id=None):
    source_id = _normalize_id(source_id)
    if target_id:
        target_id = _normalize_id(target_id)
        return f"{source_id}__TO__{target_id}"
    return source_id

def set_last_processed(source_id, msg_id, target_id=None):
    key = _make_key(source_id, target_id)
    try:
        msg_id = int(msg_id)
    except Exception:
        return

    data = load_progress()
    data["last_processed"][key] = msg_id
    save_progress(data)

def get_last_processed(source_id, target_id=None):
    key = _make_key(source_id, target_id)
    val = load_progress()["last_processed"].get(key)
    return int(val) if val is not None and isinstance(val, (int, str)) else None

# ─────────────────────────────────────────────
# BATCH CHECKPOINT TRACKING (for batch resume logic)
# ─────────────────────────────────────────────
def set_batch_checkpoint(source_id, batch_start_id, batch_end_id, target_id=None):
    """
    Track batch boundaries for resume logic.
    Ensures bot doesn't skip to next batch until current batch is 100% complete.
    
    Args:
        source_id: Source channel ID
        batch_start_id: First message ID in this batch
        batch_end_id: Last message ID in this batch
        target_id: (Optional) Target channel ID
    """
    key = _make_key(source_id, target_id)
    data = load_progress()
    
    if "batch_checkpoints" not in data:
        data["batch_checkpoints"] = {}
    
    data["batch_checkpoints"][key] = {
        "batch_start_id": int(batch_start_id),
        "batch_end_id": int(batch_end_id),
        "timestamp": __import__('datetime').datetime.now().isoformat()
    }
    save_progress(data)

def get_batch_checkpoint(source_id, target_id=None):
    """
    Get current batch boundaries.
    
    Returns:
        {"batch_start_id": X, "batch_end_id": Y} or None if no checkpoint
    """
    key = _make_key(source_id, target_id)
    data = load_progress()
    checkpoint = data.get("batch_checkpoints", {}).get(key)
    
    if checkpoint:
        return {
            "batch_start_id": int(checkpoint.get("batch_start_id", 0)),
            "batch_end_id": int(checkpoint.get("batch_end_id", 0))
        }
    return None

def get_batch_completion_percent(source_id, batch_start_id, batch_end_id, target_id=None):
    """
    Calculate what percentage of a batch has been processed (downloaded/skipped/failed).
    
    Returns:
        Completion percentage (0-100)
    """
    key = _make_key(source_id, target_id)
    data = load_progress()
    
    # Get all processed items (downloaded, skipped, or failed)
    downloaded = set(data.get("downloads", {}).get(key, []))
    skipped = set(ent.split(":")[1] for ent in data.get("skipped", {}).get("downloads", []) 
                  if isinstance(ent, str) and ent.startswith(f"{key}:") and ":" in ent)
    failed = set(ent.split(":")[1] for ent in data.get("failed", {}).get("downloads", []) 
                 if isinstance(ent, str) and ent.startswith(f"{key}:") and ":" in ent)
    
    # Convert to integers
    processed = set()
    for item in downloaded | skipped | failed:
        try:
            processed.add(int(item))
        except (ValueError, TypeError):
            pass
    
    # Count items in batch range
    batch_items = {i for i in range(batch_start_id, batch_end_id + 1)}
    items_in_batch = batch_items & processed
    
    total = len(batch_items)
    completed = len(items_in_batch)
    
    if total == 0:
        return 0
    
    return int((completed / total) * 100)

# ─────────────────────────────────────────────
# PER-ITEM DOWNLOAD TRACKING
# ─────────────────────────────────────────────
def log_item_download(post_id: int, item_num: int, total_items: int, source_key: str):
    """Log individual item downloads during album processing"""
    source_key = _normalize_id(source_key)
    try:
        post_id = int(post_id)
    except Exception:
        return
    
    item_log_key = f"{source_key}:items"
    data = load_progress()
    
    if item_log_key not in data:
        data["_item_logs"] = {}
    
    if item_log_key not in data.get("_item_logs", {}):
        data["_item_logs"] = data.get("_item_logs", {})
        data["_item_logs"][item_log_key] = []
    
    log_entry = {
        "post_id": post_id,
        "item": f"{item_num}/{total_items}",
        "timestamp": str(__import__("datetime").datetime.now())
    }
    
    data["_item_logs"][item_log_key].append(log_entry)
    # Keep only last 100 items per source for history
    if len(data["_item_logs"][item_log_key]) > 100:
        data["_item_logs"][item_log_key] = data["_item_logs"][item_log_key][-100:]
    
    save_progress(data)

print("🎯 progress_manager.py v2.2 ULTRA SAFE loaded.")