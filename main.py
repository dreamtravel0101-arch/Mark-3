# ============================================================
# Smart Telegram Uploader v4.34 TRUE ZERO FALSE-SKIP EDITION
# FULL FEATURE • TELETHON SAFE • ALBUM SAFE • RESUMABLE
# COMPATIBLE WITH:
# scanner.py, download_manager.py, reupload_manager.py,
# extractor.py, utils.py, file_handler.py
# DOES NOT USE: forward_manager.py
# ============================================================

import os
import sys
import asyncio
import signal
import shutil
import json
from pathlib import Path
from typing import List, Any
from telethon import errors
from datetime import datetime
import traceback
import threading
import time

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not installed, skip

try:
    import msvcrt
    _HAS_MSVCRT = True
except Exception:
    msvcrt = None
    _HAS_MSVCRT = False

from core.utils import BASE_DIR, ensure_dirs, create_client, ensure_join, build_caption
from core.file_handler import mark_downloaded, mark_failed, is_downloaded, mark_skipped, get_last_processed, set_last_processed, log_item_download
from core.reupload_manager import reupload_with_pool
from core.download_manager import download_media_atomic, safe_id
from core.progress_bar import SimpleProgress
from core.extractor import generate_thumbnail_sync
from core.scanner import scan_shared_messages
from core.tag_filter import get_tag_filter
from core.upload_progress import mark_uploaded, is_uploaded, mark_upload_failed, get_upload_summary
from core.caption_with_links import add_captions_to_files, process_tags, extract_links_from_text
from core.telegram_link_downloader import download_by_links

# ───────────────────────────────
# CONFIG & CONSTANTS
# ───────────────────────────────
SESSION_DIR = BASE_DIR / "sessions"
os.makedirs(str(SESSION_DIR), exist_ok=True)

MIN_FORWARD_DELAY = 2.0
DEFAULT_UPLOAD_TIMEOUT = 1800      # 30 minutes
MAX_FLOOD_WAIT = 600
VERBOSE = True
_stop = False
FORWARDED_ONLY = False  # Set True to process only forwarded posts
SKIP_EVENT = None
_KEY_LISTENER_STARTED = False

# ───────────────────────────────
# LOGGER
# ───────────────────────────────
def live_status(message: str, level: str = "INFO"):
    now = datetime.now().strftime("%H:%M:%S")
    prefix = {"INFO": "ℹ", "WARN": "⚠", "ERROR": "❌", "SUCCESS": "✅"}.get(level, "•")
    print(f"[{now}] {prefix} {message}")

# optionally preload LLM (on startup) to avoid first-inference lag
if str(os.getenv("PRELOAD_LLM", "")).strip().lower() in ("1", "true", "yes"):
    try:
        from core.llm import init_llm
        init_llm()
        live_status("Preloaded LLM model", "INFO")
    except Exception as e:
        live_status(f"LLM preload failed: {e}", "WARN")

# ───────────────────────────────
# SIGNAL HANDLER
# ───────────────────────────────
def _signal_handler(signum, frame):
    global _stop
    live_status("Shutdown requested...", "WARN")
    _stop = True

async def safe_sleep(seconds: float):
    remaining = seconds
    while remaining > 0 and not _stop:
        await asyncio.sleep(min(1.0, remaining))
        remaining -= 1.0


# ───────────────────────────────
# KEY LISTENER (press 's' to skip current upload)
# ───────────────────────────────
def _key_listener_loop():
    while True:
        try:
            if _HAS_MSVCRT and SKIP_EVENT is not None:
                if msvcrt.kbhit():
                    ch = msvcrt.getwch()
                    if ch in ("s", "S"):
                        try:
                            SKIP_EVENT.set()
                            live_status("Skip requested (s pressed)", "WARN")
                        except Exception:
                            pass
            elif _HAS_MSVCRT:
                # drain keypresses so they don't accumulate when not in upload
                if msvcrt.kbhit():
                    _ = msvcrt.getwch()
            time.sleep(0.12)
        except Exception:
            time.sleep(0.5)


def start_key_listener():
    global _KEY_LISTENER_STARTED
    if _KEY_LISTENER_STARTED:
        return
    t = threading.Thread(target=_key_listener_loop, daemon=True)
    t.start()
    _KEY_LISTENER_STARTED = True

# ───────────────────────────────
# TELETHON HELPERS
# ───────────────────────────────
def extract_first_msg_id(sent_obj: Any) -> int:
    if not sent_obj:
        return 0
    if isinstance(sent_obj, (list, tuple)):
        first = sent_obj[0] if sent_obj else None
        if isinstance(first, (list, tuple)):
            first = first[0] if first else None
        return getattr(first, "id", 0)
    return getattr(sent_obj, "id", 0)

def diagnose_upload_response(result: Any, post_id: int, target_entity: Any) -> dict:
    """Comprehensive diagnostic for upload response"""
    diagnostics = {
        "post_id": post_id,
        "target_entity_id": getattr(target_entity, "id", "unknown"),
        "target_entity_type": type(target_entity).__name__,
        "target_entity_title": getattr(target_entity, "title", "N/A"),
        "target_entity_is_supergroup": getattr(target_entity, "megagroup", False),
        "target_entity_is_forum": getattr(target_entity, "forum", False),
        "response_type": type(result).__name__,
        "response_is_none": result is None,
        "response_is_list": isinstance(result, (list, tuple)),
        "response_length": len(result) if isinstance(result, (list, tuple)) else 0,
        "extracted_msg_id": extract_first_msg_id(result),
    }
    
    # Try to get actual message from response
    if isinstance(result, (list, tuple)) and result:
        first_item = result[0] if result else None
        if first_item:
            diagnostics["first_item_type"] = type(first_item).__name__
            diagnostics["first_item_id"] = getattr(first_item, "id", None)
            diagnostics["first_item_chat_id"] = getattr(first_item, "chat_id", None)
            diagnostics["first_item_peer_id"] = getattr(first_item, "peer_id", None)
    
    return diagnostics

def make_progress_key(src: Any, tgt: Any) -> str:
    src_id = getattr(src, 'id', src) if src else "unknown_src"
    tgt_id = getattr(tgt, 'id', tgt) if tgt else "unknown_tgt"
    return f"{src_id}__TO__{tgt_id}"

def validate_file(path: str) -> bool:
    try:
        p = Path(path)
        return p.is_file() and p.stat().st_size > 0
    except:
        return False


def analyze_media_files(file_paths: List[str]) -> dict:
    """
    Analyze media files and return count and varieties.
    
    Returns: {
        'total': int,
        'photos': int,
        'videos': int,
        'documents': int,
        'other': int,
        'summary': str,  # e.g., "Album: 5 items (3 videos, 2 photos)"
        'varieties': [str]  # e.g., ['video', 'photo']
    }
    """
    if not file_paths:
        return {
            'total': 0,
            'photos': 0,
            'videos': 0,
            'documents': 0,
            'other': 0,
            'summary': 'No media',
            'varieties': []
        }
    
    stats = {
        'total': len(file_paths),
        'photos': 0,
        'videos': 0,
        'documents': 0,
        'other': 0,
    }
    
    for fpath_str in file_paths:
        ext = Path(fpath_str).suffix.lower()
        if ext in {".mp4", ".mkv", ".mov", ".ts", ".webm", ".avi", ".flv", ".m3u8"}:
            stats['videos'] += 1
        elif ext in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tiff"}:
            stats['photos'] += 1
        elif ext in {".pdf", ".doc", ".docx", ".txt", ".zip", ".rar", ".7z", ".tar", ".gz"}:
            stats['documents'] += 1
        else:
            stats['other'] += 1
    
    # Build varieties list
    varieties = []
    if stats['videos'] > 0:
        varieties.append(f"{stats['videos']} video{'s' if stats['videos'] > 1 else ''}")
    if stats['photos'] > 0:
        varieties.append(f"{stats['photos']} photo{'s' if stats['photos'] > 1 else ''}")
    if stats['documents'] > 0:
        varieties.append(f"{stats['documents']} doc{'s' if stats['documents'] > 1 else ''}")
    if stats['other'] > 0:
        varieties.append(f"{stats['other']} other")
    
    # Build summary
    if stats['total'] == 1:
        media_type = "Single media"
        if stats['videos'] > 0:
            media_type = "Single video"
        elif stats['photos'] > 0:
            media_type = "Single photo"
        elif stats['documents'] > 0:
            media_type = "Single document"
        summary = media_type
    else:
        variety_text = ", ".join(varieties)
        summary = f"Album: {stats['total']} items ({variety_text})"
    
    stats['summary'] = summary
    stats['varieties'] = varieties
    
    return stats

def split_large_file(file_path: str, max_size: int = 1073741824) -> List[str]:  # 1GB
    """
    Split a large video file using FFmpeg stream copy (preserves quality, fast).
    Returns list of chunk file paths.
    """
    import subprocess
    from core.reupload_manager import FFPROBE_BIN, FFMPEG_BIN
    from core.progress_bar import SimpleProgress
    import math

    p = Path(file_path)
    total_bytes = p.stat().st_size
    if total_bytes <= max_size:
        return [file_path]

    # estimate number of parts for progress bar
    est_parts = math.ceil(total_bytes / max_size)
    progress = None
    try:
        # Initialize progress with total file size in bytes, not part count
        progress = SimpleProgress(total_bytes, prefix="🔧 Splitting")
        progress.set_stage(f"{est_parts} chunks")
    except Exception:
        progress = None

    # Use FFprobe to determine total duration; use configured binary path
    try:
        cmd = [
            FFPROBE_BIN or "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            str(p)
        ]
        print(f"[Split] running ffprobe: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=True)
        total_duration = float(result.stdout.strip())
    except Exception as exc:
        # log details for debugging
        print(f"[Split] ffprobe failed for {p.name}: {exc}")
        try:
            if hasattr(exc, 'stdout') and exc.stdout:
                print(f"[Split] ffprobe stdout: {exc.stdout}")
            if hasattr(exc, 'stderr') and exc.stderr:
                print(f"[Split] ffprobe stderr: {exc.stderr}")
        except Exception:
            pass

        # try a fallback with ffmpeg -i to parse duration from stderr
        total_duration = None
        try:
            ffmpeg_cmd = [FFMPEG_BIN or "ffmpeg", "-i", str(p)]
            print(f"[Split] attempting ffmpeg duration parse: {' '.join(ffmpeg_cmd)}")
            res2 = subprocess.run(ffmpeg_cmd, capture_output=True, text=True, timeout=30)
            import re
            m = re.search(r"Duration: (\d+):(\d+):(\d+(?:\.\d+)?)", res2.stderr or "")
            if m:
                h = int(m.group(1))
                mn = int(m.group(2))
                sec = float(m.group(3))
                total_duration = h * 3600 + mn * 60 + sec
                print(f"[Split] parsed duration via ffmpeg: {total_duration}s")
        except Exception as e2:
            print(f"[Split] ffmpeg fallback also failed: {e2}")

        if total_duration is None:
            # Fallback: binary split (slow re-encode but works)
            print(f"⚠ FFmpeg duration detection failed, using binary split for {p.name}")
            chunks = []
            chunk_size = max_size
            with open(file_path, 'rb') as f:
                part_num = 1
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    chunk_path = p.with_name(f"{p.stem}_part{part_num}{p.suffix}")
                    with open(chunk_path, 'wb') as cf:
                        cf.write(chunk)
                    chunks.append(str(chunk_path))
                    part_num += 1
                    bytes_processed = (part_num - 1) * chunk_size
                    if progress:
                        progress.update(current_bytes=bytes_processed, total_bytes=total_bytes)
            if progress:
                progress.done()
            return chunks
    
    # Calculate chunk duration and split
    chunk_duration = (total_duration * max_size) / p.stat().st_size
    chunks = []
    part_num = 1
    start = 0
    
    while start < total_duration:
        chunk_path = p.with_name(f"{p.stem}_part{part_num}{p.suffix}")
        end = min(start + chunk_duration, total_duration)
        
        # FFmpeg stream copy (no re-encode) with seek optimization
        from core.reupload_manager import FFMPEG_BIN
        cmd = [
            FFMPEG_BIN or "ffmpeg", "-y", "-v", "error",
            "-ss", str(start),
            "-to", str(end),
            "-i", str(p),
            "-c", "copy",  # NO re-encoding
            "-avoid_negative_ts", "make_zero",
            str(chunk_path)
        ]
        print(f"[Split] running ffmpeg: {' '.join(cmd)}")
        try:
            subprocess.run(cmd, check=True, timeout=300)
            chunks.append(str(chunk_path))
            chunk_size = chunk_path.stat().st_size if chunk_path.exists() else 0
            bytes_processed = sum(Path(c).stat().st_size for c in chunks if Path(c).exists())
            print(f"[Split] ✓ Created {chunk_path.name} (duration: {end - start:.1f}s, size: {chunk_size / (1024**2):.1f}MB)")
            if progress:
                progress.update(current_bytes=bytes_processed, total_bytes=total_bytes)
        except Exception as e:
            # show stdout/stderr if available
            print(f"[Split] ✗ Failed {chunk_path.name}: {e}")
            try:
                if hasattr(e, 'stdout') and e.stdout:
                    print(f"[Split] ffmpeg stdout: {e.stdout}")
                if hasattr(e, 'stderr') and e.stderr:
                    print(f"[Split] ffmpeg stderr: {e.stderr}")
            except Exception:
                pass
            if progress:
                progress.done()
            return [file_path]  # Fallback to original if split fails
        
        start = end
        part_num += 1
    
    if progress:
        progress.done()
    return chunks
# Captions are now built by `core.utils.build_caption`.
# The original implementation was moved to that shared utility so both the
# relay engine and the standalone download manager can reuse it.


# helper used across multiple functions

def _summarize_group(msgs):
    kinds = []
    for m in msgs:
        if getattr(m, "video", None):
            kinds.append("video")
        elif getattr(m, "photo", None):
            kinds.append("photo")
        elif getattr(m, "document", None):
            kinds.append("document")
        elif getattr(m, "media", None):
            kinds.append("media")
        elif getattr(m, "text", None):
            kinds.append("text")
        else:
            kinds.append("unknown")
    return kinds

# ───────────────────────────────
# RELAY / DOWNLOAD → REUPLOAD ENGINE
# ───────────────────────────────
async def download_and_reupload_multi(clients: List[Any], src_entity, tgt_entity, selected_post_ids: List[int] = None, start_id_override: int = None, forwarded_only: bool = False, apply_exclusions: bool = True):

    global SKIP_EVENT

    main_client = clients[0]
    progress_key = make_progress_key(src_entity, tgt_entity)
    live_status(f"Starting relay → progress key: {progress_key}")
    
    # Diagnostic: confirm entities at function entry
    print(f"\n[RELAY_START_DIAG] ═════════════════════════════════════════")
    print(f"[RELAY_START_DIAG] Source Entity: ID={src_entity.id}, Type={type(src_entity).__name__}, Title='{getattr(src_entity, 'title', 'N/A')}'")
    print(f"[RELAY_START_DIAG] Target Entity: ID={tgt_entity.id}, Type={type(tgt_entity).__name__}, Title='{getattr(tgt_entity, 'title', 'N/A')}'")
    print(f"[RELAY_START_DIAG] Target is Supergroup: {getattr(tgt_entity, 'megagroup', False)}")
    print(f"[RELAY_START_DIAG] Target is Forum: {getattr(tgt_entity, 'forum', False)}")
    print(f"[RELAY_START_DIAG] ═════════════════════════════════════════\n")
    print(f"[RELAY_START_DIAG] Will upload TO: '{getattr(tgt_entity, 'title', 'Unknown')}' (ID: {tgt_entity.id})")

    # ═════════════════════════════════════════════════════════════════
    # RECOVERY SYSTEM: Check for failed uploads with local files
    # ═════════════════════════════════════════════════════════════════
    from core.file_handler import load_progress
    progress_data = load_progress()
    failed_uploads = set()
    
    # Get all failed post entries (check BOTH downloads and uploads sections)
    # (old code marked uploads as downloads, new code marks them correctly)
    try:
        for section in ["uploads", "downloads"]:  # Check both for backward compatibility
            for ent in progress_data.get("failed", {}).get(section, []):
                if isinstance(ent, str) and ent.startswith(f"{progress_key}:"):
                    try:
                        post_id = int(ent.split(":", 1)[1])
                        failed_uploads.add(post_id)
                    except Exception:
                        pass
    except Exception:
        pass
    
    # Check which failed posts still have files locally
    recovery_queue = []
    relay_root_base = Path("storage/relay") / str(src_entity.id)
    if failed_uploads and relay_root_base.exists():
        for post_id in failed_uploads:
            post_dir = relay_root_base / str(post_id)
            if post_dir.exists() and list(post_dir.glob("*")):  # Has files
                recovery_queue.append(post_id)
                live_status(f"[RECOVERY] Found failed post {post_id} with local files - will upload first", "WARN")
    
    # Priority: Upload failed posts first before processing new ones
    # DEBUG: Show what was found
    print(f"[DEBUG] Failed uploads in progress.json: {progress_data.get('failed', {}).get('uploads', [])}")
    print(f"[DEBUG] Failed uploads after filtering: {failed_uploads}")
    print(f"[DEBUG] Recovery queue (failed with local files): {recovery_queue}")
    
    if recovery_queue:
        live_status(f"[RECOVERY] ⚠ Found {len(recovery_queue)} failed uploads with local files - processing first...", "WARN")
    
    # Continue with normal startup logic
    # Decide whether to delete the original message from the source
    # channel after it has been successfully relayed.
    # DEFAULT: DO NOT delete (safer default)
    # To enable auto-delete, set environment variable: AUTO_DELETE_ORIGINAL=1/true/yes
    # To ensure no delete: AUTO_DELETE_ORIGINAL=0/false/no
    delete_after_upload = False
    env_del = os.getenv("AUTO_DELETE_ORIGINAL")
    if env_del is not None:
        delete_after_upload = str(env_del).strip().lower() in ("1", "true", "yes")
        live_status(f"delete_after_upload set via AUTO_DELETE_ORIGINAL={env_del}", "INFO")
    else:
        live_status("Auto-delete disabled (default) - source posts will NOT be deleted after upload", "INFO")
        live_status("To enable: set AUTO_DELETE_ORIGINAL=1 environment variable", "INFO")

        # Ask if they want to delete original posts from source after successful upload
        # Only ask if environment variable is NOT set
        try:
            resp = input("Delete original posts from source channel after successful upload? (y/n, default n): ").strip().lower()
            if resp in ("y", "yes"): 
                delete_after_upload = True
                live_status(f"Original posts WILL be deleted after successful upload", "INFO")
            else:
                # Default to no deletion
                delete_after_upload = False
                live_status(f"Original posts will NOT be deleted after successful upload", "INFO")
        except Exception:
            delete_after_upload = False

    # Ask if they want to recover deleted posts from admin log
    recover_deleted = False
    try:
        resp = input("Also recover recently deleted posts from admin log? (y/n, default n): ").strip().lower()
        if resp in ("y", "yes"): recover_deleted = True
    except Exception:
        pass

    # Local file deletion behaviour: the relay engine will always purge the
    # downloaded files after successfully uploading them unless archive support
    # is enabled *and* the archival itself fails.  Historically this was
    # controlled by DELETE_LOCAL_FILES and an auto‑disable in Codespaces, but
    # users typically want the local cache removed.  We therefore ignore that
    # logic and enforce cleanup here.
    import os as _os
    delete_local_files = True  # hard enforcement for relay mode

    # Load exclusion list (same as scanner) - only if apply_exclusions is True
    from core.scanner import _extract_forward_ids
    from pathlib import Path as PathlibPath
    import json as json_lib
    exclude_file = PathlibPath(__file__).parent / "config" / "scanner_exclude.json"
    excluded_ids = set()
    
    if apply_exclusions:
        if exclude_file.exists():
            try:
                raw = json_lib.loads(exclude_file.read_text(encoding="utf-8"))
                if isinstance(raw, list):
                    for item in raw:
                        s = str(item).strip()
                        if s.lstrip("-").isdigit():
                            excluded_ids.add(int(s))
            except Exception as e:
                print(f"[Relay] Failed reading exclude file: {e}")
        
        if excluded_ids:
            print(f"[Relay] ✓ Loaded {len(excluded_ids)} excluded IDs: {sorted(excluded_ids)}")
    else:
        print(f"[Relay] ⚠️  Exclusion list BYPASSED - processing ALL posts including excluded sources")

    processed = 0
    skipped = 0
    skipped_excluded = 0
    all_groups = []
    current_group = []
    current_group_id = None

    # First, try to process any pending downloads from progress.json that are older than last_processed
    pending_downloads = list(progress_data.get("downloads", {}).get(progress_key, []))
    # Filter to ensure all items are integers (handle corrupted data)
    pending_downloads = [x for x in pending_downloads if isinstance(x, int)]

    # retry any previously failed downloads
    try:
        for ent in progress_data.get("failed", {}).get("downloads", []):
            if isinstance(ent, str) and ent.startswith(f"{progress_key}:"):
                try:
                    mid = int(ent.split(":", 1)[1])
                    if mid not in pending_downloads:
                        pending_downloads.append(mid)
                except Exception:
                    pass
    except Exception:
        pass

    # determine resume point
    last_id = get_last_processed(src_entity.id) or 0
    
    # ═════════════════════════════════════════════════════════════════
    # BATCH CHECKPOINT LOGIC: Don't move to next batch until current is done
    # ═════════════════════════════════════════════════════════════════
    from core.file_handler import get_batch_checkpoint, get_batch_completion_percent
    batch_checkpoint = get_batch_checkpoint(src_entity.id, tgt_entity.id if tgt_entity else None)
    
    if batch_checkpoint:
        # Check if previous batch was completed
        batch_start = batch_checkpoint["batch_start_id"]
        batch_end = batch_checkpoint["batch_end_id"]
        completion = get_batch_completion_percent(src_entity.id, batch_start, batch_end, 
                                                  tgt_entity.id if tgt_entity else None)
        
        if completion < 100:
            # Previous batch NOT done - go back to resume it
            live_status(f"Previous batch ({batch_start}-{batch_end}) incomplete at {completion}% - resuming from {batch_start}", "WARN")
            last_id = batch_start - 1  # Will fetch starting from batch_start
        else:
            # Previous batch complete - move to next batch
            live_status(f"Previous batch ({batch_start}-{batch_end}) complete - moving to next batch", "SUCCESS")
            # last_id stays at where we left off
    
    # allow caller parameter to override
    if start_id_override is not None:
        last_id = start_id_override
        live_status(f"Overriding resume ID via argument: {last_id}")
    else:
        # also support environment variable for interactive resumes
        env_val = os.environ.get("RELAY_START_ID")
        if env_val:
            try:
                override = int(env_val)
                last_id = override
                live_status(f"Overriding resume ID from environment: {last_id}")
            except Exception:
                pass
        else:
            try:
                inp = input(f"Enter minimum message ID to resume from (current {last_id}, leave empty to keep): ").strip()
                if inp:
                    last_id = int(inp)
                    live_status(f"Resuming from user-specified ID: {last_id}")
            except Exception:
                pass

    # identify any message IDs earlier than last_processed that are not tracked at all
    # (neither downloaded, skipped nor failed) – these likely indicate manual removal
    
    if last_id and last_id > 0:
        # build known sets
        downloaded_list = progress_data.get("downloads", {}).get(progress_key, [])
        downloaded_set = set(x for x in downloaded_list if isinstance(x, int))
        skipped_set = set()
        for ent in progress_data.get("skipped", {}).get("downloads", []):
            if isinstance(ent, str) and ent.startswith(f"{progress_key}:"):
                try:
                    skipped_set.add(int(ent.split(":", 1)[1]))
                except Exception:
                    pass
        failed_set = set()
        for ent in progress_data.get("failed", {}).get("downloads", []):
            if isinstance(ent, str) and ent.startswith(f"{progress_key}:"):
                try:
                    failed_set.add(int(ent.split(":", 1)[1]))
                except Exception:
                    pass
        all_known = downloaded_set | skipped_set | failed_set
        if all_known:
            min_known = min(all_known)
        else:
            min_known = last_id
        # compute missing IDs in the interval [min_known, last_id)
        missing = []
        for i in range(min_known, last_id):
            if i not in all_known:
                missing.append(i)
        for mid in missing:
            if mid not in pending_downloads:
                pending_downloads.append(mid)
        if missing:
            live_status(f"Detected {len(missing)} untracked IDs from earlier than last_processed; requeuing: {missing}")

    # ═════════════════════════════════════════════════════════════════
    # IF SPECIFIC POST IDS SELECTED (from URL), ONLY PROCESS THOSE
    # ═════════════════════════════════════════════════════════════════
    if selected_post_ids:
        live_status(f"[URL_PARSE] Only processing selected message IDs: {selected_post_ids}", "INFO")
        try:
            selected_msgs = await main_client.get_messages(src_entity, ids=selected_post_ids)
            selected_msgs = [m for m in selected_msgs if m is not None]
            selected_msgs = sorted(selected_msgs, key=lambda m: m.id)
            
            # DEBUG: Show structure of selected messages to identify topics
            for msg in selected_msgs:
                topic_root = getattr(msg, 'reply_to_msg_id', None)
                print(f"\n[DOWNLOAD_ROUTING_DEBUG] Message {msg.id}:")
                print(f"  - is_reply: {getattr(msg, 'is_reply', False)}")
                print(f"  - reply_to_msg_id: {topic_root}")
                if topic_root and topic_root > 0:
                    print(f"  → Message is in TOPIC {topic_root} (will relay to that topic)")
                else:
                    print(f"  → Message is in GENERAL topic (will relay to general)")
            print()
            
            # Group the selected messages just like normal processing
            current_group = []
            current_group_id = None
            for msg in selected_msgs:
                if not (getattr(msg, "media", None) or getattr(msg, "text", None)):
                    continue
                gid = getattr(msg, "grouped_id", None)
                if gid:
                    if current_group_id != gid:
                        if current_group:
                            all_groups.append(current_group)
                        current_group = [msg]
                        current_group_id = gid
                    else:
                        current_group.append(msg)
                else:
                    if current_group:
                        all_groups.append(current_group)
                        current_group = []
                        current_group_id = None
                    all_groups.append([msg])
            
            if current_group:
                all_groups.append(current_group)
            
            live_status(f"[URL_PARSE] ✅ Loaded {len(all_groups)} group(s) from selected messages", "SUCCESS")
        except Exception as e:
            live_status(f"[URL_PARSE] ✗ Failed to fetch selected messages: {e}", "ERROR")
    else:
        # Normal flow: process pending and iterate through all messages
        
        if pending_downloads:
            live_status(f"Found {len(pending_downloads)} pending downloads to process first")
            # Fetch these specific messages
            pending_msgs = []
            try:
                # Get messages by ID in batches  
                msg_batch = await main_client.get_messages(src_entity, pending_downloads)
                msg_batch = [m for m in msg_batch if m is not None]
                pending_msgs = sorted(msg_batch, key=lambda m: m.id)
            except Exception as e:
                live_status(f"Failed to fetch pending messages: {e}", "WARN")
                pending_msgs = []
            
            # Process pending messages
            for msg in pending_msgs:
                if _stop:
                    break
                if is_downloaded(msg.id, progress_key):
                    continue
                if not (getattr(msg, "media", None) or getattr(msg, "text", None)):
                    continue
                # Add to groups for processing
                gid = getattr(msg, "grouped_id", None)
                if gid:
                    if current_group_id != gid:
                        if current_group:
                            all_groups.append(current_group)
                        current_group = [msg]
                        current_group_id = gid
                    else:
                        current_group.append(msg)
                else:
                    if current_group:
                        all_groups.append(current_group)
                        current_group = []
                        current_group_id = None
                    all_groups.append([msg])

        # last_id may have been adjusted earlier (override or prompt)
        live_status(f"Resuming from post ID: {last_id}")

        # ═════════════════════════════════════════════════════════════════
        # BATCH-BASED MESSAGE FETCHING & PROCESSING
        # Fetch 100-200 messages, download+upload them, then repeat
        # This avoids 10-15min delay of collecting all IDs first
        # ═════════════════════════════════════════════════════════════════
        BATCH_SIZE = 150  # Fetch and process 150 messages at a time
        batch_num = 0
        total_batches_estimate = "unknown"

        live_status(f"Starting batch processing (batch size: {BATCH_SIZE})", "INFO")
        live_status(f"Each batch will be: FETCHED → GROUPED → DOWNLOADED → UPLOADED → next batch", "INFO")
        
        # Get the peer to determine total message count
        try:
            peer_info = await main_client.get_entity(src_entity)
            total_messages = getattr(peer_info, 'messages_count', None)
            if total_messages and last_id > 0:
                total_batches_estimate = (total_messages - last_id) // BATCH_SIZE + 1
                live_status(f"Estimated batches to process: ~{total_batches_estimate}", "INFO")
        except Exception:
            pass

        # Iterate through messages in batches
        offset_id = 0
        fetching_complete = False
        
        while not fetching_complete and not _stop:
            batch_num += 1
            batch_groups = []  # Groups for THIS batch only
            current_group = []
            current_group_id = None
            
            # ─────────────────────────────────────────────────────────────
            # STEP 1: FETCH batch of 100-200 messages
            # ─────────────────────────────────────────────────────────────
            try:
                batch_messages = []
                async for msg in main_client.iter_messages(src_entity, reverse=True, min_id=last_id, offset_id=offset_id, limit=BATCH_SIZE):
                    batch_messages.append(msg)
                
                if not batch_messages:
                    fetching_complete = True
                    live_status(f"[Batch {batch_num}] ✅ Fetching complete - no more messages", "SUCCESS")
                    break
                
                live_status(f"[Batch {batch_num}] ✅ Fetched {len(batch_messages)} messages", "SUCCESS")
                
                # ═════════════════════════════════════════════════════════════════
                # SET BATCH CHECKPOINT: Track this batch for resume logic
                # ═════════════════════════════════════════════════════════════════
                batch_start_id = batch_messages[0].id
                batch_end_id = batch_messages[-1].id
                from core.file_handler import set_batch_checkpoint
                set_batch_checkpoint(src_entity.id, batch_start_id, batch_end_id, tgt_entity.id if tgt_entity else None)
                live_status(f"[Batch {batch_num}] 📌 Batch checkpoint set: {batch_start_id}-{batch_end_id}", "INFO")
                
                # ─────────────────────────────────────────────────────────────
                # STEP 2: GROUP messages in this batch
                # ─────────────────────────────────────────────────────────────
                for msg in batch_messages:
                    if not (getattr(msg, "media", None) or getattr(msg, "text", None)):
                        # Message has no media and no text - silent skip (service message likely)
                        print(f"[Relay] ⊘ Skipping message {msg.id} (no media or text - service message)")
                        mark_skipped(f"{progress_key}:{msg.id}", mode="downloads")
                        continue
                    
                    # Check if forwarded/shared (only if forwarded_only is enabled)
                    if forwarded_only:
                        is_forwarded = bool(getattr(msg, "forward", None) or getattr(msg, "fwd_from", None))
                        if not is_forwarded:
                            continue

                    # Check if forwarded from excluded source
                    if excluded_ids:
                        try:
                            orig_ids, sender_ids = _extract_forward_ids(msg)
                            # Skip if original source is excluded
                            if any((int(x) in excluded_ids) for x in orig_ids if x is not None):
                                print(f"[Relay] ✓ SKIP excluded source: post {msg.id}, orig_ids={orig_ids}")
                                skipped_excluded += 1
                                continue
                            # Skip if forward sender is excluded
                            if any((int(x) in excluded_ids) for x in sender_ids if x is not None):
                                print(f"[Relay] ✓ SKIP excluded sender: post {msg.id}, sender_ids={sender_ids}")
                                skipped_excluded += 1
                                continue
                        except Exception as e:
                            if False:  # debug only
                                print(f"[Relay] Error checking exclusions: {e}")
                            pass

                    gid = getattr(msg, "grouped_id", None)
                    if VERBOSE or True:  # Always show for debugging
                        print(f"[Relay] Message {msg.id}: grouped_id={gid}, media={bool(getattr(msg, 'media', None))}")
                    if gid:
                        if current_group_id != gid:
                            if current_group:
                                batch_groups.append(current_group)
                            current_group = [msg]
                            current_group_id = gid
                        else:
                            current_group.append(msg)
                    else:
                        if current_group:
                            batch_groups.append(current_group)
                            current_group = []
                            current_group_id = None
                        batch_groups.append([msg])
                
                if current_group:
                    batch_groups.append(current_group)

                live_status(f"[Batch {batch_num}] Grouped into {len(batch_groups)} post groups", "INFO")
                
                # Update offset for next batch
                if batch_messages:
                    offset_id = batch_messages[-1].id
                
                # Brief pause before processing
                await safe_sleep(0.5)
                
            except Exception as e:
                live_status(f"[Batch {batch_num}] Error fetching batch: {e}", "WARN")
                await safe_sleep(2.0)
                continue

            # ─────────────────────────────────────────────────────────────
            # STEP 3: DOWNLOAD & UPLOAD all groups in this batch (IMMEDIATE)
            # ─────────────────────────────────────────────────────────────
            live_status(f"[Batch {batch_num}] 🚀 Starting download & upload for {len(batch_groups)} groups...", "INFO")
            
            batch_post_counter = 0
            for group in batch_groups:
                if _stop:
                    break
                    
                batch_post_counter += 1
                root_msg = group[0]
                post_id = root_msg.id

                if is_downloaded(post_id, progress_key):
                    skipped += 1
                    if VERBOSE:
                        live_status(f"[Batch {batch_num}] Skipping already processed post {post_id}")
                    continue

                kinds = _summarize_group(group)
                kind_label = "album" if len(group) > 1 else "single"
                live_status(
                    f"[Batch {batch_num}] [POST ID: {post_id}] ({batch_post_counter}/{len(batch_groups)}) {kind_label} ({len(group)} items); types: {', '.join(kinds)}"
                )

                # Archive all messages in this group for guaranteed recovery
                from core.archive_manager import archive_message_during_relay
                for msg in group:
                    archive_message_during_relay(msg, progress_key)

                relay_root = Path("storage/relay") / str(src_entity.id) / str(post_id)
                relay_root.mkdir(parents=True, exist_ok=True)
                thumbs_dir = relay_root / "thumbs"
                thumbs_dir.mkdir(exist_ok=True)

                # Build caption first - this preserves all text, links, and formatting
                caption = build_caption(group)

                # optionally post-process caption through a local LLM (set USE_LLM_CAPTION=1)
                if str(os.getenv("USE_LLM_CAPTION", "")).strip().lower() in ("1", "true", "yes"):
                    try:
                        # import inside block so the heavy libs only load if needed
                        from core.llm import generate_async
                        prompt = (
                            "Rewrite the following Telegram caption to be concise while "
                            "preserving all meaning and formatting.\n\n" + caption
                        )
                        # limit tokens to avoid runaway generation
                        caption = await generate_async(prompt, max_tokens=512)
                        live_status("[Batch {batch_num}] Caption rewritten by LLM", "INFO")
                    except Exception as e:
                        live_status(f"[Batch {batch_num}] LLM caption rewrite failed: {e}", "WARN")

                # Determine if all messages already have downloaded files.
                # We look for existing filenames starting with the safe_id of each message.
                msg_ids = {safe_id(m) for m in group}
                existing_files = [f for f in relay_root.iterdir() if f.is_file() and validate_file(str(f))]
                existing_ids = {f.stem.split("_")[0] for f in existing_files}

                media_files = []
                if msg_ids and msg_ids.issubset(existing_ids):
                    # fast path: all message media already present
                    media_files = [str(f) for f in existing_files if f.suffix.lower() in {'.mp4', '.mkv', '.mov', '.jpg', '.png', '.pdf', '.webp', '.webm', '.avi'} and not str(f).endswith('.partial')]
                    analysis = analyze_media_files(media_files)
                    live_status(f"[Batch {batch_num}] [DOWNLOAD] ✅ {analysis['summary']}")
                    # debug: list files and map to message ids
                    try:
                        for f in existing_files:
                            live_status(f"[Batch {batch_num}]   ✓ File: {f.name}")
                        missing = msg_ids - existing_ids
                        if missing:
                            live_status(f"[Batch {batch_num}]   ⚠ Missing ids: {', '.join(missing)}", "WARN")
                    except Exception:
                        pass
                else:
                    # Download media for each message. _download_single will skip files that
                    # already exist locally, so we don't re-fetch previously saved items.
                    try:
                        dl_files = await download_media_atomic(clients, group, relay_root, post_num=batch_post_counter, total_posts=len(batch_groups), post_id=post_id)
                        media_files = [f for f in (dl_files or []) if validate_file(f) and not f.endswith('.partial')]
                        
                        # Log individual items downloaded  
                        if media_files and len(group) > 1:
                            for item_idx in range(1, len(group) + 1):
                                log_item_download(post_id, item_idx, len(group), progress_key)
                        
                        # Mark as downloaded immediately after download completes (before upload)
                        if media_files:
                            set_last_processed(src_entity.id, post_id)
                            live_status(f"[Batch {batch_num}] [PROGRESS] ✅ Download checkpoint saved for post {post_id}")
                    except Exception as e:
                        live_status(f"[Batch {batch_num}] Download failed for {post_id}: {e}", "ERROR")
                        mark_failed(f"{progress_key}:{post_id}")
                        continue
                    if media_files:
                        analysis = analyze_media_files(media_files)
                        live_status(f"[Batch {batch_num}] [DOWNLOAD] ✅ {analysis['summary']}")

                # Skip only if BOTH media and caption are empty
                if not media_files and not caption.strip():
                    mark_skipped(f"{progress_key}:{post_id}")
                    live_status(f"[Batch {batch_num}] Skipping empty post {post_id}")
                    continue
                
                # Save caption to file for reference (even if we have media)
                if caption.strip():
                    caption_file = relay_root / "caption.txt"
                    try:
                        with open(caption_file, "w", encoding="utf-8") as f:
                            f.write(caption)
                        live_status(f"[Batch {batch_num}] Saved caption/links for post {post_id} to caption.txt")
                    except Exception as e:
                        live_status(f"[Batch {batch_num}] Failed to save caption/links for post {post_id}: {e}", "WARN")
                
                # Save reply_to_msg_id for forum topic routing (recovery if upload fails)
                reply_to_msg_id = getattr(root_msg, 'reply_to_msg_id', None)
                if reply_to_msg_id and reply_to_msg_id > 0:
                    reply_to_file = relay_root / "reply_to_msg_id.txt"
                    try:
                        with open(reply_to_file, "w", encoding="utf-8") as f:
                            f.write(str(reply_to_msg_id))
                        live_status(f"[Batch {batch_num}] Saved reply_to_msg_id {reply_to_msg_id} for post {post_id} (topic routing)")
                    except Exception as e:
                        live_status(f"[Batch {batch_num}] Failed to save reply_to_msg_id for post {post_id}: {e}", "WARN")

                # Split large videos
                expanded_files = []
                for mf in media_files:
                    p = Path(mf)
                    if p.suffix.lower() in {'.mp4', '.mkv', '.mov', '.avi', '.ts', '.webm'} and p.stat().st_size > 1073741824:
                        live_status(f"[Batch {batch_num}] Splitting large video {p.name} ({p.stat().st_size / (1024**3):.2f}GB)")
                        chunks = split_large_file(mf)
                        expanded_files.extend(chunks)
                    else:
                        expanded_files.append(mf)
                media_files = expanded_files

                upload_items = []
                for fpath_str in media_files:
                    fpath = Path(fpath_str)
                    thumb = None
                    ext = fpath.suffix.lower()
                    if ext in {".mp4", ".mkv", ".mov", ".ts", ".webm", ".avi"}:
                        thumb_path = thumbs_dir / f"{fpath.stem}.jpg"
                        if not thumb_path.exists():
                            try:
                                generate_thumbnail_sync(str(fpath), str(thumb_path))
                            except Exception as ex:
                                live_status(f"[Batch {batch_num}] Thumb gen failed {fpath.name}: {ex}", "WARN")
                        if thumb_path.exists() and thumb_path.stat().st_size > 0:
                            thumb = str(thumb_path)
                    upload_items.append({"path": str(fpath), "thumb": thumb})

                # attach saved caption file as document ONLY when there are no media files
                # (when there are media files, the caption is applied to them directly)
                if caption.strip() and not media_files:
                    caption_file = relay_root / "caption.txt"
                    if caption_file.exists():
                        # prepend to ensure it appears first in any chunk
                        upload_items.insert(0, {"path": str(caption_file), "thumb": None})

                # Display upload summary with media count and varieties
                if upload_items:
                    media_paths = [item["path"] for item in upload_items]
                    analysis = analyze_media_files(media_paths)
                    
                    # Calculate upload size
                    total_upload_size = sum(Path(p).stat().st_size for p in media_paths if Path(p).exists())
                    def format_size(bytes_val):
                        for unit in ['B', 'KB', 'MB', 'GB']:
                            if bytes_val < 1024:
                                return f"{bytes_val:.2f}{unit}"
                            bytes_val /= 1024
                        return f"{bytes_val:.2f}TB"
                    
                    size_str = format_size(total_upload_size)
                    live_status(f"[Batch {batch_num}] 📤 Uploading {len(upload_items)} items ({size_str}) for post {post_id}", "INFO")
                else:
                    live_status(f"[Batch {batch_num}] 📝 Uploading caption/links only for post {post_id}", "INFO")

                try:
                    cancel_event = threading.Event()
                    start_key_listener()
                    SKIP_EVENT = cancel_event
                    
                    # Upload retry with exponential backoff
                    result = None
                    attempt = 0
                    max_backoff = 300
                    
                    try:
                        while True:  # Keep retrying until success
                            attempt += 1
                            try:
                                if upload_items:
                                    if attempt > 1:
                                        backoff_secs = min(5 * (2 ** (attempt - 2)), max_backoff)
                                        live_status(f"[Batch {batch_num}] Upload retry #{attempt} for post {post_id} - waiting {int(backoff_secs)}s before retry...", "WARN")
                                        await safe_sleep(backoff_secs)
                                    
                                    live_status(f"[Batch {batch_num}] [UPLOAD_ATTEMPT #{attempt}] Starting upload for post {post_id}...", "INFO")
                                    
                                    reply_to_msg_id = getattr(root_msg, 'reply_to_msg_id', None)
                                    
                                    if reply_to_msg_id and reply_to_msg_id > 0:
                                        live_status(f"[Batch {batch_num}] [FORUM_ROUTING] ✅ Post {post_id}: routing to topic (reply_to={reply_to_msg_id})", "INFO")
                                    
                                    result = await reupload_with_pool(
                                        clients,
                                        tgt_entity,
                                        upload_items,
                                        caption=caption,
                                        reply_to=reply_to_msg_id,
                                        timeout=DEFAULT_UPLOAD_TIMEOUT,
                                        cancel_event=cancel_event,
                                    )
                                    live_status(f"[Batch {batch_num}] [UPLOAD_ATTEMPT #{attempt}] ✅ Upload succeeded on attempt {attempt}", "SUCCESS")
                                    break  # Success! Exit retry loop
                                elif caption.strip():
                                    # Only caption/links, no media
                                    reply_to_msg_id = getattr(root_msg, 'reply_to_msg_id', None)
                                    result = await clients[0].send_message(tgt_entity, caption, reply_to=reply_to_msg_id)
                                    break
                                else:
                                    result = None
                                    break
                            except KeyboardInterrupt:
                                # Allow user to cancel current upload and mark as skipped
                                mark_skipped(f"{progress_key}:{post_id}")
                                live_status(f"[Batch {batch_num}] Upload cancelled by user — skipped post {post_id}", "WARN")
                                raise
                            except RuntimeError as e:
                                # Upload cancelled via key listener
                                if "cancel" in str(e).lower():
                                    mark_skipped(f"{progress_key}:{post_id}")
                                    live_status(f"[Batch {batch_num}] Upload skipped by user — post {post_id}", "WARN")
                                    raise
                                # Other RuntimeError - log and retry
                                live_status(f"[Batch {batch_num}] [UPLOAD_ATTEMPT #{attempt}] ✗ Failed: {e}", "WARN")
                            except Exception as e:
                                # Any other error - log and retry
                                live_status(f"[Batch {batch_num}] [UPLOAD_ATTEMPT #{attempt}] ✗ Failed: {e}", "WARN")
                    finally:
                        SKIP_EVENT = None

                    # Check if upload was successful
                    sent_id = extract_first_msg_id(result)
                    if sent_id > 0:
                        mark_downloaded(post_id, progress_key)
                        set_last_processed(src_entity.id, post_id)
                        processed += 1
                        live_status(f"[Batch {batch_num}] ✅ Uploaded post {post_id} → target msg {sent_id}", "SUCCESS")
                        
                        if delete_after_upload:
                            try:
                                await main_client.delete_messages(src_entity, post_id)
                                print(f"[CLEANUP] Deleted original post {post_id} from source channel (SUCCESS)")
                            except Exception as ex:
                                print(f"[CLEANUP] Failed to delete original post {post_id}: {ex}")
                        else:
                            print(f"[CLEANUP] Skipped deletion for post {post_id} (user opted out)")
                        
                        if delete_local_files:
                            try:
                                shutil.rmtree(str(relay_root))
                                live_status(f"[Batch {batch_num}] [CLEANUP] ✅ Local files deleted for post {post_id}")
                            except Exception as e:
                                live_status(f"[Batch {batch_num}] [CLEANUP] ⚠ Failed to delete local files: {e}", "WARN")
                    elif result:
                        # Some uploaders may return truthy success without a message object
                        mark_downloaded(post_id, progress_key)
                        set_last_processed(src_entity.id, post_id)
                        processed += 1
                        live_status(f"[Batch {batch_num}] Uploaded post {post_id} → (no msg id returned) — marked done", "SUCCESS")
                        if delete_after_upload:
                            try:
                                await main_client.delete_messages(src_entity, post_id)
                                print(f"[CLEANUP] Deleted original post {post_id} from source channel (SUCCESS)")
                            except Exception as ex:
                                print(f"[CLEANUP] Failed to delete original post {post_id}: {ex}")
                        else:
                            print(f"[CLEANUP] Skipped deletion for post {post_id} (user opted out)")
                        if delete_local_files:
                            try:
                                shutil.rmtree(str(relay_root))
                                live_status(f"[Batch {batch_num}] [CLEANUP] ✅ Local files deleted for post {post_id}")
                            except Exception as e:
                                live_status(f"[Batch {batch_num}] [CLEANUP] ⚠ Failed to delete local files: {e}", "WARN")
                    else:
                        mark_failed(f"{progress_key}:{post_id}", mode="uploads")
                        live_status(f"[Batch {batch_num}] Upload returned no valid message id for {post_id}", "ERROR")
                except errors.FloodWaitError as e:
                    wait = min(e.seconds, MAX_FLOOD_WAIT)
                    live_status(f"[Batch {batch_num}] Flood wait {wait}s", "WARN")
                    await safe_sleep(wait)
                except Exception as e:
                    mark_failed(f"{progress_key}:{post_id}", mode="uploads")
                    live_status(f"[Batch {batch_num}] Upload error for {post_id}: {e}", "ERROR")
                    traceback.print_exc()

                await safe_sleep(MIN_FORWARD_DELAY)
            
            live_status(f"[Batch {batch_num}] ✅ BATCH COMPLETE - Processed {batch_post_counter} groups from {len(batch_messages)} messages", "SUCCESS")
            
            # ═════════════════════════════════════════════════════════════
            # ADVANCE LAST_PROCESSED: Even if some messages were silently skipped (no media/text),
            # mark the highest message ID in this batch as processed to avoid re-fetching
            # ═════════════════════════════════════════════════════════════
            if batch_messages:
                highest_msg_id = max(msg.id for msg in batch_messages)
                set_last_processed(src_entity.id, highest_msg_id)
                live_status(f"[Batch {batch_num}] 📌 Advanced last_processed to {highest_msg_id} (marks whole batch as seen)", "INFO")
            
            print()  # Blank line between batches
            await safe_sleep(1.0)  # Pause before next batch

    # ═════════════════════════════════════════════════════════════════
    # PROCESS FAILED UPLOADS FIRST (Recovery system)
    # ═════════════════════════════════════════════════════════════════
    recovery_counter = 0
    for failed_post_id in recovery_queue:
        if _stop:
            break
        recovery_counter += 1
        relay_root = relay_root_base / str(failed_post_id)
        
        live_status(f"[RECOVERY {recovery_counter}/{len(recovery_queue)}] Uploading failed post {failed_post_id}...", "WARN")
        
        # Collect media files from local storage (skip .partial incomplete downloads)
        media_files = [str(f) for f in relay_root.glob("*") if f.is_file() and f.suffix.lower() in {'.mp4', '.mkv', '.mov', '.jpg', '.png', '.pdf', '.webp', '.webm', '.avi'} and not str(f).endswith('.partial')]
        
        if not media_files:
            live_status(f"[RECOVERY] ✗ No files found for post {failed_post_id}, skipping...", "WARN")
            continue
        
        live_status(f"[RECOVERY] Found {len(media_files)} media files to upload", "INFO")
        
        # Try to upload from local cache
        try:
            upload_items = [{"path": f} for f in media_files]
            caption = ""  # Try to load caption if exists
            caption_file = relay_root / "caption.txt"
            if caption_file.exists():
                caption = caption_file.read_text(encoding="utf-8", errors="ignore")
            
            # Try to restore reply_to_msg_id for forum topic routing during recovery
            reply_to_msg_id = None
            reply_to_file = relay_root / "reply_to_msg_id.txt"
            if reply_to_file.exists():
                try:
                    reply_to_msg_id = int(reply_to_file.read_text(encoding="utf-8", errors="ignore").strip())
                    if reply_to_msg_id > 0:
                        live_status(f"[RECOVERY] Found reply_to_msg_id {reply_to_msg_id} for post {failed_post_id}, will route to topic", "INFO")
                except Exception:
                    pass
            
            result = await reupload_with_pool(
                clients,
                tgt_entity,
                upload_items,
                caption=caption,
                reply_to=reply_to_msg_id,
                timeout=DEFAULT_UPLOAD_TIMEOUT,
            )
            
            sent_id = extract_first_msg_id(result)
            if sent_id > 0:
                live_status(f"[RECOVERY] ✅ Post {failed_post_id} uploaded successfully (msg {sent_id})", "SUCCESS")
                mark_downloaded(failed_post_id, progress_key)
                set_last_processed(src_entity.id, failed_post_id)
                
                # Cleanup
                if delete_local_files:
                    try:
                        shutil.rmtree(str(relay_root))
                        live_status(f"[RECOVERY] ✅ Local files deleted for post {failed_post_id}", "SUCCESS")
                    except Exception as e:
                        live_status(f"[RECOVERY] ⚠ Failed to delete local files: {e}", "WARN")
            else:
                live_status(f"[RECOVERY] ⚠ Post {failed_post_id} upload still failing, will retry next startup", "WARN")
        except Exception as e:
            live_status(f"[RECOVERY] ✗ Post {failed_post_id} upload error: {e}", "ERROR")
    
    if recovery_queue:
        live_status(f"[RECOVERY] Completed {recovery_counter}/{len(recovery_queue)} failed uploads", "INFO")

    # If user requested, also recover deleted posts from admin log
    if recover_deleted:
        live_status("Checking admin log for recently deleted posts...")
        try:
            from core.scanner import scan_deleted_messages
            # if user supplied a start_id we want to ignore previous download marks
            ignore = last_id is not None
            bypass_check = last_id is not None
            deleted_messages, _, _ = await scan_deleted_messages(
                main_client, src_entity, 
                limit=100,  # Check last 100 admin log events
                forwarded_only=True,  # Only recover forwarded posts
                ignore_downloaded=ignore,
            )
            
            if deleted_messages:
                live_status(f"Found {len(deleted_messages)} potentially recoverable deleted posts")
                
                # Process deleted messages similar to regular messages
                for msg in deleted_messages:
                    if _stop:
                        break

                    post_id = msg.id

                    # Check if already processed
                    if not bypass_check and is_downloaded(post_id, progress_key):
                        continue

                    live_status(f"Recovering deleted post {post_id}")

                    # Create relay directory
                    relay_root = Path("storage/relay") / str(src_entity.id) / str(post_id)
                    relay_root.mkdir(parents=True, exist_ok=True)
                    thumbs_dir = relay_root / "thumbs"
                    thumbs_dir.mkdir(exist_ok=True)

                    # Download media from deleted message
                    media_files = []
                    if hasattr(msg, 'media') and msg.media:
                        try:
                            media_path = relay_root / f"media_{post_id}"
                            if hasattr(msg.media, 'photo'):
                                media_path = media_path.with_suffix('.jpg')
                            elif hasattr(msg.media, 'document'):
                                mime_type = getattr(msg.media, 'mime_type', '')
                                if mime_type:
                                    if 'video' in mime_type:
                                        media_path = media_path.with_suffix('.mp4')
                                    elif 'image' in mime_type:
                                        media_path = media_path.with_suffix('.jpg')
                                    elif 'audio' in mime_type:
                                        media_path = media_path.with_suffix('.mp3')
                                    else:
                                        media_path = media_path.with_suffix('.bin')
                                else:
                                    media_path = media_path.with_suffix('.bin')

                            await main_client.download_media(msg, str(media_path))
                            if media_path.exists() and media_path.stat().st_size > 0:
                                media_files.append(str(media_path))
                                live_status(f"Downloaded media for deleted post {post_id}")
                            else:
                                live_status(f"Failed to download media for deleted post {post_id}", "WARN")
                                continue
                        except Exception as e:
                            live_status(f"Media download failed for deleted post {post_id}: {e}", "ERROR")
                            continue
                    else:
                        live_status(f"Deleted post {post_id} has no media, will upload text only")

                    # Prepare upload items
                    upload_items = []
                    for fpath_str in media_files:
                        fpath = Path(fpath_str)
                        thumb = None
                        if fpath.suffix.lower() in {".mp4", ".mkv", ".mov", ".ts", ".webm", ".avi"}:
                            thumb_path = thumbs_dir / f"{fpath.stem}.jpg"
                            if not thumb_path.exists():
                                try:
                                    generate_thumbnail_sync(str(fpath), str(thumb_path))
                                except Exception as ex:
                                    live_status(f"Thumb gen failed {fpath.name}: {ex}", "WARN")
                            if thumb_path.exists() and thumb_path.stat().st_size > 0:
                                thumb = str(thumb_path)
                        upload_items.append({"path": str(fpath), "thumb": thumb})

                    # Build caption
                    caption = build_caption([msg])

                    if not upload_items and not caption.strip():
                        live_status(f"Skipping deleted post {post_id} - no media or text")
                        continue

                    # Upload
                    try:
                        cancel_event = threading.Event()
                        start_key_listener()
                        SKIP_EVENT = cancel_event
                        try:
                            result = await reupload_with_pool(
                                clients,
                                tgt_entity,
                                upload_items,
                                caption=caption,
                                timeout=DEFAULT_UPLOAD_TIMEOUT,
                                cancel_event=cancel_event,
                            )
                        except RuntimeError as e:
                            if "cancel" in str(e).lower():
                                live_status(f"Recovery skipped by user — deleted post {post_id}", "WARN")
                                continue
                            raise
                        finally:
                            SKIP_EVENT = None

                        sent_id = extract_first_msg_id(result)
                        if sent_id > 0:
                            mark_downloaded(post_id, progress_key)
                            set_last_processed(src_entity.id, post_id)
                            processed += 1
                            live_status(f"Recovered deleted post {post_id} → target msg {sent_id}", "SUCCESS")
                        else:
                            mark_failed(f"{progress_key}:{post_id}")
                            live_status(f"Upload failed for deleted post {post_id}", "ERROR")

                    except errors.FloodWaitError as e:
                        wait = min(e.seconds, MAX_FLOOD_WAIT)
                        live_status(f"Flood wait {wait}s", "WARN")
                        await safe_sleep(wait)
                    except Exception as e:
                        mark_failed(f"{progress_key}:{post_id}")
                        live_status(f"Recovery error for deleted post {post_id}: {e}", "ERROR")

                    await safe_sleep(MIN_FORWARD_DELAY)
            else:
                live_status("No recoverable deleted posts found in admin log")
                
        except Exception as e:
            live_status(f"Admin log recovery failed: {e}", "ERROR")

    # Professional relay summary report
    total_posts = processed + skipped
    success_rate = (processed / total_posts * 100) if total_posts > 0 else 0
    print(f"\n╔══════════════════════════════════════════════════════════════════╗")
    print(f"║ 🎉 RELAY SESSION COMPLETE - Expert Summary Report")
    print(f"╠══════════════════════════════════════════════════════════════════╣")
    print(f"║ 📊 Total Posts Processed:    {total_posts:>3}")
    print(f"║ ✅ Successfully Uploaded:    {processed:>3} ({success_rate:>6.1f}%)")
    print(f"║ ⏭️  Skipped:                  {skipped:>3} ({100-success_rate:>6.1f}%)")
    if skipped_excluded > 0:
        print(f"║ 🚫 Excluded:                 {skipped_excluded:>3}")
    print(f"╠══════════════════════════════════════════════════════════════════╣")
    print(f"║ 🚀 Status: COMPLETE")
    print(f"╚══════════════════════════════════════════════════════════════════╝\n")

# ───────────────────────────────
# RECOVER DELETED POSTS FROM ADMIN LOG
# ───────────────────────────────
async def recover_deleted_posts(clients: List[Any], src_entity, tgt_entity):
    """
    Recover deleted posts from admin log and reupload them.
    Requires admin rights to the source channel.
    Behaviour: when given a `start_id` it will recover deleted messages with
    message ID >= start_id, processing them from oldest → newest. Already
    processed messages (tracked in `progress.json`) are skipped and progress
    is updated as posts are recovered.
    """
    global SKIP_EVENT
    from core.scanner import scan_deleted_messages

    main_client = clients[0]
    progress_key = make_progress_key(src_entity, tgt_entity)
    live_status(f"Starting deleted post recovery → progress key: {progress_key}")

    # Determine start_id (env var overrides interactive input)
    start_id = None
    env_val = os.environ.get("RECOVER_START_ID")
    if env_val:
        try:
            start_id = int(env_val)
            live_status(f"Using RECOVER_START_ID environment variable: {start_id}", "INFO")
        except ValueError:
            live_status(f"RECOVER_START_ID='{env_val}' is not a valid integer, ignoring", "WARN")
            start_id = None
    else:
        try:
            start_id_input = input(
                "Enter minimum message ID to recover from (leave empty to process all deleted posts). "
                "Messages with ID >= this value will be recovered even if already marked downloaded: ").strip()
            if start_id_input:
                # sanitize: remove any non-digits
                import re
                digits = re.sub(r"\D", "", start_id_input)
                if digits:
                    start_id = int(digits)
                else:
                    raise ValueError("no digits")
        except ValueError:
            live_status(f"Invalid minimum ID '{start_id_input}' entered, recovering all deleted messages", "WARN")
            start_id = None

    if start_id is not None:
        live_status(f"Will only recover deleted messages with ID >= {start_id}", "INFO")

    # if a start_id is specified we want to recover everything regardless of forwarding status
    if start_id is not None:
        forwarded_only = False
    else:
        forwarded_only = input("Only recover forwarded/shared posts? (y/n, default n): ").strip().lower() in ("y", "yes")
    skip_thumbs = input("Skip thumbnail generation for videos? (y/n, default n): ").strip().lower() in ("y", "yes")

    batch_size = int(os.getenv("RECOVER_BATCH_SIZE", "200"))
    total_processed = 0
    total_skipped = 0
    admin_max_id = 0

    # Collect matching deleted messages across admin-log pages
    collected = []
    # remember the smallest message ID we see in any event, even if filtered
    min_msg_id_overall = None
    while not _stop:
        live_status(f"Scanning admin log for deleted messages (admin_max_id={admin_max_id}, batch_size={batch_size})...")
        try:
            deleted_messages, min_event_id, batch_min_msg = await scan_deleted_messages(
                main_client, src_entity,
                limit=batch_size, max_id=admin_max_id, min_id=start_id,
                forwarded_only=forwarded_only,
                ignore_downloaded=(start_id is not None),
            )
        except Exception as e:
            live_status(f"Failed to scan admin log: {e}", "ERROR")
            break

        if deleted_messages:
            live_status(f"Found {len(deleted_messages)} deleted messages in this batch")
            collected.extend(deleted_messages)
        else:
            live_status("No deleted messages in this batch")

        # update the overall min message id seen (batch_min_msg may be None)
        if batch_min_msg is not None:
            if min_msg_id_overall is None or batch_min_msg < min_msg_id_overall:
                min_msg_id_overall = batch_min_msg

        if min_event_id is not None:
            admin_max_id = min_event_id - 1
            continue
        else:
            break

    if not collected:
        # Try archive-based recovery as fallback
        live_status("Admin log empty, attempting archive-based recovery...", "INFO")

        from core.archive_manager import recover_from_archive, get_archived_message_count
        archived_count = get_archived_message_count(src_entity.id)

        if archived_count > 0:
            live_status(f"Found {archived_count} archived messages, scanning for recoverable posts...", "INFO")

            # Try to recover messages from archive starting from start_id or recent messages
            recovered_from_archive = []
            scan_start = start_id or (max(1, min_msg_id_overall or 0) if min_msg_id_overall else 1)
            scan_limit = 1000  # Don't scan infinitely

            for msg_id in range(scan_start, scan_start + scan_limit):
                if _stop:
                    break

                recovered_msg = recover_from_archive(src_entity.id, msg_id)
                if recovered_msg:
                    # Apply the same filters as admin log recovery
                    if forwarded_only and not (recovered_msg.forward or getattr(recovered_msg, 'fwd_from', None)):
                        continue

                    # Check if already downloaded (unless override requested)
                    if start_id is None and is_downloaded(recovered_msg.id, progress_key):
                        continue

                    # Check if has media or text
                    if not (getattr(recovered_msg, "media", None) or getattr(recovered_msg, "text", None) or getattr(recovered_msg, "caption", None)):
                        continue

                    recovered_from_archive.append(recovered_msg)

                    if len(recovered_from_archive) >= 50:  # Limit batch size
                        break

            if recovered_from_archive:
                collected = recovered_from_archive
                live_status(f"✅ Recovered {len(collected)} messages from archive backup!", "SUCCESS")
            else:
                live_status("No messages found in archive either", "WARN")
        else:
            live_status("No archived messages available for recovery", "WARN")

        # If still no messages after archive recovery
        if not collected:
            # Warn about possible retention issues if the user asked for a starting ID
            if start_id is not None and min_msg_id_overall is not None and min_msg_id_overall > start_id:
                live_status(
                    f"Admin log only contains deletion events for messages >= {min_msg_id_overall}; "
                    f"messages with ID < {min_msg_id_overall} cannot be recovered (they may have been pruned).",
                    "WARN",
                )
            live_status("No recoverable deleted messages found in admin log or archive", "SUCCESS")
            return

    # Deduplicate and sort oldest->newest
    unique = {m.id: m for m in collected}
    deleted_messages_all = sorted(unique.values(), key=lambda m: m.id)

    # If start_id was provided, warn if the earliest recovered message is still above it
    if start_id is not None and min_msg_id_overall is not None and min_msg_id_overall > start_id:
        live_status(
            f"Note: the earliest deletion event available in the admin log is for message {min_msg_id_overall}, "
            "so posts earlier than this cannot be recovered.",
            "WARN",
        )

    # Filter already processed and group by album
    from collections import OrderedDict
    groups = OrderedDict()
    bypass_check = start_id is not None
    for msg in deleted_messages_all:
        if not bypass_check and is_downloaded(msg.id, progress_key):
            total_skipped += 1
            live_status(f"Skipping already processed deleted post {msg.id}")
            continue
        gid = getattr(msg, 'grouped_id', None) or f"single_{msg.id}"
        groups.setdefault(gid, []).append(msg)
    if bypass_check:
        live_status("Note: start_id specified, bypassing download-status checks for recovered posts", "INFO")

    # Process groups sequentially (oldest → newest)
    for gid, group in groups.items():
        if _stop:
            break

        post_id = min(msg.id for msg in group)
        # examine kinds
        kinds = _summarize_group(group)
        kind_label = "album" if len(group) > 1 else "single"
        live_status(f"Processing deleted post {post_id} - {kind_label} ({len(group)} items); types: {', '.join(kinds)}")

        relay_root = Path("storage/relay") / str(src_entity.id) / str(post_id)
        thumbs_dir = relay_root / "thumbs"
        thumbs_dir.mkdir(parents=True, exist_ok=True)

        # Download media for the group (using multiple accounts for speed)
        media_files = []
        for msg in group:
            try:
                dl = await download_media_atomic(clients, [msg], relay_root)
                media_files.extend([f for f in (dl or []) if validate_file(f)])
            except Exception as e:
                live_status(f"Download failed for {msg.id}: {e}", "WARN")
                continue

        # Classify and possibly split large videos
        video_exts = {".mp4", ".mkv", ".mov", ".ts", ".webm", ".avi", ".m4v", ".3gp", ".flv", ".wmv"}
        photo_exts = {".jpg", ".jpeg", ".png", ".webp", ".gif"}

        expanded_files = []
        for mf in media_files:
            p = Path(mf)
            if p.suffix.lower() in video_exts and p.stat().st_size > 1073741824:
                live_status(f"Splitting large video {p.name} ({p.stat().st_size / (1024**3):.2f}GB)")
                try:
                    chunks = split_large_file(mf)
                    expanded_files.extend(chunks)
                except Exception as e:
                    live_status(f"Split failed {p.name}: {e}", "WARN")
                    expanded_files.append(mf)
            else:
                expanded_files.append(mf)
        media_files = expanded_files

        # Prepare upload items and thumbs
        upload_items = []
        for fpath_str in media_files:
            fpath = Path(fpath_str)
            thumb = None
            if fpath.suffix.lower() in video_exts and not skip_thumbs:
                thumb_path = thumbs_dir / f"{fpath.stem}.jpg"
                if not thumb_path.exists():
                    try:
                        generate_thumbnail_sync(str(fpath), str(thumb_path))
                    except Exception as ex:
                        live_status(f"Thumb gen failed {fpath.name}: {ex}", "WARN")
                if thumb_path.exists() and thumb_path.stat().st_size > 0:
                    thumb = str(thumb_path)
            upload_items.append({"path": str(fpath), "thumb": thumb})

        caption = build_caption(group)

        if not upload_items and not caption.strip():
            live_status(f"Skipping deleted post {post_id} - no media or text")
            continue

        # Upload
        try:
            cancel_event = threading.Event()
            start_key_listener()
            SKIP_EVENT = cancel_event
            try:
                result = await reupload_with_pool(
                    clients,
                    tgt_entity,
                    upload_items,
                    caption=caption,
                    timeout=DEFAULT_UPLOAD_TIMEOUT,
                    cancel_event=cancel_event,
                )
            except RuntimeError as e:
                if "cancel" in str(e).lower():
                    live_status(f"Recovery skipped by user — deleted post {post_id}", "WARN")
                    continue
                raise
            finally:
                SKIP_EVENT = None

            sent_id = extract_first_msg_id(result)
            if sent_id > 0:
                for msg in group:
                    mark_downloaded(msg.id, progress_key)
                set_last_processed(src_entity.id, post_id)
                total_processed += len(group)
                live_status(f"Recovered deleted post {post_id} → target msg {sent_id}", "SUCCESS")
            else:
                mark_failed(f"{progress_key}:{post_id}")
                live_status(f"Upload failed for deleted post {post_id}", "ERROR")

        except errors.FloodWaitError as e:
            wait = min(e.seconds, MAX_FLOOD_WAIT)
            live_status(f"Flood wait {wait}s", "WARN")
            await asyncio.sleep(wait)
            continue
        except Exception as e:
            live_status(f"Recovery failed for deleted post {post_id}: {e}", "ERROR")
            mark_failed(f"{progress_key}:{post_id}")
            continue

    live_status(f"Deleted post recovery complete! Total recovered: {total_processed}, total skipped: {total_skipped}", "SUCCESS")
    # If a start_id was provided, now perform a normal relay from that ID to catch any non-deleted posts
    if start_id is not None:
        live_status(f"Also relaying normal posts starting at {start_id} to fill any gaps (forwarded_only={forwarded_only})")
        try:
            await download_and_reupload_multi(clients, src_entity, tgt_entity, start_id_override=start_id, forwarded_only=forwarded_only)
        except Exception as e:
            live_status(f"Fallback relay after recovery failed: {e}", "ERROR")

# ───────────────────────────────
# ARCHIVE MANAGEMENT MENU
# ───────────────────────────────
async def archive_management_menu(clients: List[Any]):
    """Menu for managing message archives and backups."""
    ARCHIVE_MENU = """
📦 Archive Management
1) Rebuild archives from channel history
2) View archive statistics
3) Clean up old archives
4) Back to main menu
Choose: """

    while not _stop:
        try:
            choice = input(ARCHIVE_MENU).strip()

            if choice == "1":
                # Rebuild archives from channel history
                channel_str = input("Channel to archive (username/link/ID): ").strip()
                limit_str = input("Number of recent messages to archive (default 1000): ").strip()
                limit = int(limit_str) if limit_str.isdigit() else 1000

                channel = await ensure_join(clients[0], channel_str)
                if not channel:
                    continue

                live_status(f"Rebuilding archive for {channel.id} with last {limit} messages...", "INFO")

                from core.archive_manager import rebuild_from_history
                await rebuild_from_history(clients[0], channel, limit=limit)

                from core.archive_manager import get_archived_message_count
                count = get_archived_message_count(channel.id)
                live_status(f"✅ Archive rebuilt. Now contains {count} messages.", "SUCCESS")

            elif choice == "2":
                # View archive statistics
                from core.archive_manager import list_archived_chats, get_archived_message_count

                archived_chats = list_archived_chats()
                if not archived_chats:
                    live_status("No archived chats found.", "INFO")
                    continue

                live_status(f"📊 Archive Statistics ({len(archived_chats)} chats):", "INFO")
                total_messages = 0
                for chat_id in archived_chats:
                    count = get_archived_message_count(chat_id)
                    total_messages += count
                    print(f"  Chat {chat_id}: {count} messages")

                live_status(f"📈 Total archived messages: {total_messages}", "INFO")

            elif choice == "3":
                # Clean up old archives
                days_str = input("Delete archives older than X days (default 30): ").strip()
                days = int(days_str) if days_str.isdigit() else 30

                from core.archive_manager import cleanup_old_archives
                cleanup_old_archives(days)
                live_status(f"✅ Cleaned up archives older than {days} days.", "SUCCESS")

            elif choice == "4":
                break

            else:
                print("Invalid choice.")

        except Exception as e:
            live_status(f"Archive management error: {e}", "ERROR")


# ───────────────────────────────
# INTERACTIVE SESSION
# ───────────────────────────────
UPLOAD_MENU = """
1) Upload folder → Channel
2) Download & Reupload (Only the forwarded posts)
3) Download & Reupload (All)
4) Recover deleted posts from admin log
5) Archive Management (Backup & Recovery)
6) Upload with Tags & Captions (Bangla support)
7) Download by Telegram Link
8) Exit
Choose: """

async def interactive_session(clients: List[Any]):
    if not clients:
        return

    me = await clients[0].get_me()
    live_status(f"Logged in as {me.first_name} (@{me.username or 'no-username'})", "SUCCESS")
    # Ensure key listener is running so user can press 's' to skip uploads
    start_key_listener()
    live_status("Press 's' during an upload to skip the current post.", "INFO")

    while not _stop:
        try:
            choice = input(UPLOAD_MENU).strip()
            if choice == "1":
                folder = input("Folder path: ").strip()
                target_str = input("Target channel/username/link: ").strip()
                target = await ensure_join(clients[0], target_str)
                if not target:
                    continue
                progress_key = make_progress_key("folder", target.id)
                files = [os.path.join(root, f) for root, _, fnames in os.walk(folder) for f in fnames if f.lower().endswith((".mp4",".mkv",".mov",".jpg",".jpeg",".png",".pdf",".webp",".gif",".ts",".webm",".avi"))]
                for fpath in files:
                    if _stop: break
                    if is_downloaded(fpath, progress_key):
                        live_status(f"Skip already done: {os.path.basename(fpath)}")
                        continue
                    thumb = None
                    if Path(fpath).suffix.lower() in {".mp4",".mkv",".mov",".ts",".webm",".avi"}:
                        tpath = Path(fpath).with_suffix(".jpg")
                        if not tpath.exists():
                            generate_thumbnail_sync(fpath, str(tpath))
                        if tpath.exists():
                            thumb = str(tpath)
                    
                    # Display caption on separate line before upload
                    caption_text = os.path.basename(fpath)
                    caption_display = caption_text[:80]  # Show up to 80 chars
                    if len(caption_text) > 80:
                        caption_display = caption_display[:77] + "..."
                    print(f"⬆️ {caption_display}")
                    sys.stdout.flush()  # Ensure it displays immediately
                    
                    res = await reupload_with_pool(clients, target, [{"path": fpath, "thumb": thumb}], caption=os.path.basename(fpath), timeout=DEFAULT_UPLOAD_TIMEOUT)
                    if extract_first_msg_id(res) > 0:
                        sent_id = extract_first_msg_id(res)
                        mark_downloaded(fpath, progress_key)
                        # Track upload for resume capability (using file path hash as pseudo-msg-id)
                        try:
                            file_hash = str(abs(hash(fpath)) % 1000000)  # Create unique ID from file path
                            mark_uploaded("folder", target.id, file_hash, sent_id)
                        except Exception:
                            pass
                        live_status(f"Uploaded {os.path.basename(fpath)}", "SUCCESS")
                    else:
                        live_status(f"Failed {os.path.basename(fpath)}", "ERROR")

            elif choice == "2":
                # Download & Reupload ONLY forwarded posts (follows scanner.py + exclusion list)
                src_str = input("Source channel: ").strip()
                tgt_str = input("Target channel: ").strip()
                src = await ensure_join(clients[0], src_str)
                tgt = await ensure_join(clients[0], tgt_str)
                if not src or not tgt:
                    continue
                # Diagnostic: confirm target entity
                print(f"[CHANNEL_DIAG] Source: ID={src.id}, Type={type(src).__name__}, Name={getattr(src, 'title', 'N/A')}")
                print(f"[CHANNEL_DIAG] Target: ID={tgt.id}, Type={type(tgt).__name__}, Name={getattr(tgt, 'title', 'N/A')}")
                live_status("Relay mode: FORWARDED POSTS ONLY", "INFO")
                live_status("  • Filtering: FORWARDED/SHARED posts only (skips member & admin original posts)", "INFO")
                live_status("  • Exclusions: ENABLED (respects scanner_exclude.json)", "INFO")
                live_status("  • Upload history: CHECKED (won't re-upload already uploaded posts)", "INFO")
                await download_and_reupload_multi(clients, src, tgt, forwarded_only=True, apply_exclusions=True)

            elif choice == "3":
                # Download & Reupload ALL posts (including member/admin original posts, no exclusions)
                src_str = input("Source channel: ").strip()
                tgt_str = input("Target channel: ").strip()
                src = await ensure_join(clients[0], src_str)
                tgt = await ensure_join(clients[0], tgt_str)
                if not src or not tgt:
                    continue
                # Diagnostic: confirm target entity
                print(f"[CHANNEL_DIAG] Source: ID={src.id}, Type={type(src).__name__}, Name={getattr(src, 'title', 'N/A')}")
                print(f"[CHANNEL_DIAG] Target: ID={tgt.id}, Type={type(tgt).__name__}, Name={getattr(tgt, 'title', 'N/A')}")
                live_status("Relay mode: ALL POSTS", "INFO")
                live_status("  • Filtering: NONE (gets ALL posts: forwarded + member/admin original posts)", "INFO")
                live_status("  • Exclusions: DISABLED (no scanner_exclude.json filtering)", "INFO")
                live_status("  • Upload history: CHECKED (won't re-upload already uploaded posts)", "INFO")
                await download_and_reupload_multi(clients, src, tgt, forwarded_only=False, apply_exclusions=False)

            elif choice == "4":
                src_str = input("Source channel (where posts were deleted): ").strip()
                tgt_str = input("Target channel (where to reupload): ").strip()
                src = await ensure_join(clients[0], src_str)
                tgt = await ensure_join(clients[0], tgt_str)
                if not src or not tgt:
                    continue
                await recover_deleted_posts(clients, src, tgt)

            elif choice == "5":
                # Archive Management Menu
                await archive_management_menu(clients)

            elif choice == "6":
                # Upload with Tags & Captions (Bangla support)
                folder = input("Folder path (videos to caption): ").strip()
                caption = input("Caption text: ").strip()
                tags_input = input("Tags (comma-separated): ").strip()
                position = input("Caption position [top/bottom] (default: top): ").strip().lower()
                output_dir = input("Output folder (default: same as input): ").strip()
                timestamp = input("Include timestamp? [y/n] (default: y): ").strip().lower()
                
                if not folder or not os.path.isdir(folder):
                    live_status("Invalid folder path", "ERROR")
                    continue
                
                tags = process_tags(tags_input) if tags_input else []
                position = position if position in ["top", "bottom"] else "top"
                include_ts = timestamp != "n"
                
                # Get list of video files
                video_files = [
                    os.path.join(root, f) 
                    for root, _, fnames in os.walk(folder) 
                    for f in fnames 
                    if f.lower().endswith((".mp4", ".mkv", ".mov", ".avi", ".webm"))
                ]
                
                if not video_files:
                    live_status("No video files found in folder", "WARN")
                    continue
                
                live_status(f"Processing {len(video_files)} video(s) with caption...", "INFO")
                results = add_captions_to_files(
                    video_files,
                    caption,
                    tags,
                    output_dir=output_dir if output_dir else None,
                    position=position,
                    include_timestamp=include_ts
                )
                
                success_count = sum(1 for _, success in results if success)
                live_status(f"Captioning complete: {success_count}/{len(results)} successful", "SUCCESS")
                
                # Optionally upload the captioned videos
                upload_captioned = input("Upload captioned videos? [y/n] (default: n): ").strip().lower()
                if upload_captioned == "y":
                    target_str = input("Target channel/username: ").strip()
                    target = await ensure_join(clients[0], target_str)
                    if target:
                        progress_key = make_progress_key("captioned", target.id)
                        for output_file, success in results:
                            if success and os.path.exists(output_file):
                                if _stop: break
                                
                                # Check if already uploaded (basic upload logic)
                                if is_downloaded(output_file, progress_key):
                                    live_status(f"Skip already done: {os.path.basename(output_file)}")
                                    continue
                                
                                # Generate thumbnail (basic upload logic)
                                thumb = None
                                fpath_obj = Path(output_file)
                                if fpath_obj.suffix.lower() in {".mp4", ".mkv", ".mov", ".ts", ".webm", ".avi"}:
                                    tpath = fpath_obj.with_stem(fpath_obj.stem + "_thumb").with_suffix(".jpg")
                                    if not tpath.exists():
                                        generate_thumbnail_sync(output_file, str(tpath))
                                    if tpath.exists():
                                        thumb = str(tpath)
                                
                                # Display caption with formatting (basic upload logic)
                                caption_text = os.path.basename(output_file)
                                caption_display = caption_text[:80]
                                if len(caption_text) > 80:
                                    caption_display = caption_display[:77] + "..."
                                print(f"⬆️ {caption_display}")
                                sys.stdout.flush()
                                
                                # Upload using basic logic
                                res = await reupload_with_pool(
                                    clients, target,
                                    [{"path": output_file, "thumb": thumb}],
                                    caption=os.path.basename(output_file),
                                    timeout=DEFAULT_UPLOAD_TIMEOUT
                                )
                                
                                # Track upload like basic upload (basic upload logic)
                                if extract_first_msg_id(res) > 0:
                                    sent_id = extract_first_msg_id(res)
                                    mark_downloaded(output_file, progress_key)  # Mark as processed
                                    # Track upload for resume capability
                                    try:
                                        file_hash = str(abs(hash(output_file)) % 1000000)
                                        mark_uploaded("captioned", target.id, file_hash, sent_id)
                                    except Exception:
                                        pass
                                    live_status(f"Uploaded {os.path.basename(output_file)}", "SUCCESS")
                                else:
                                    live_status(f"Failed {os.path.basename(output_file)}", "ERROR")

            elif choice == "7":
                # Download by Telegram Link
                link_input = input("Telegram links or message IDs (comma/newline separated): ").strip()
                default_channel = input("Default channel (if not in link): ").strip()
                output_dir = input("Output folder (default: downloads): ").strip()
                
                if not link_input:
                    live_status("No links provided", "WARN")
                    continue
                
                live_status("Starting download from links...", "INFO")
                
                # Create progress bar for downloads
                progress_bar = SimpleProgress(total_items=1)
                
                # Progress callback using SimpleProgress bar
                def progress_callback(current, total):
                    if total > 0:
                        progress_bar.update(current_bytes=current, total_bytes=total)
                
                results = await download_by_links(
                    clients[0],
                    link_input,
                    default_channel=default_channel if default_channel else None,
                    output_dir=output_dir if output_dir else "downloads",
                    on_progress=progress_callback
                )
                
                success_count = sum(1 for _, success, _ in results if success)
                downloaded_files = [f for f, success, _ in results if success]
                
                live_status(f"Download complete: {success_count}/{len(results)} successful", "SUCCESS")
                
                # Optionally upload downloaded files
                if downloaded_files:
                    upload_choice = input("Upload downloaded files to channel? [y/n] (default: n): ").strip().lower()
                    if upload_choice == "y":
                        target_str = input("Target channel/username: ").strip()
                        target = await ensure_join(clients[0], target_str)
                        if target:
                            progress_key = make_progress_key("downloaded", target.id)
                            
                            # Prepare all files for album upload (instead of one-by-one)
                            files_to_upload = []
                            for file_path in downloaded_files:
                                if is_downloaded(file_path, progress_key):
                                    live_status(f"Skip already uploaded: {os.path.basename(file_path)}")
                                    continue
                                
                                # Generate thumbnail for videos
                                thumb = None
                                fpath_obj = Path(file_path)
                                if fpath_obj.suffix.lower() in {".mp4", ".mkv", ".mov", ".ts", ".webm", ".avi"}:
                                    tpath = fpath_obj.with_stem(fpath_obj.stem + "_thumb").with_suffix(".jpg")
                                    if not tpath.exists():
                                        generate_thumbnail_sync(file_path, str(tpath))
                                    if tpath.exists():
                                        thumb = str(tpath)
                                
                                files_to_upload.append({"path": file_path, "thumb": thumb})
                            
                            if not files_to_upload:
                                live_status("No new files to upload", "WARN")
                                continue
                            
                            # Display upload status for album
                            if len(files_to_upload) > 1:
                                print(f"⬆️ Uploading {len(files_to_upload)} files as album...")
                            else:
                                print(f"⬆️ {os.path.basename(files_to_upload[0]['path'])}...")
                            sys.stdout.flush()
                            
                            # Upload ALL files as a grouped album in one call
                            # Try to load caption from the caption file saved during download
                            caption_text = f"📥 Downloaded album ({len(files_to_upload)} items)"
                            
                            # Look for caption file associated with first downloaded file
                            out_dir_path = Path(output_dir if output_dir else "downloads")
                            first_file_path = Path(files_to_upload[0]['path'])
                            caption_file = out_dir_path / f"_caption_{first_file_path.stem}.txt"
                            
                            # Try to read the original caption
                            if caption_file.exists():
                                try:
                                    with open(str(caption_file), 'r', encoding='utf-8') as f:
                                        loaded_caption = f.read().strip()
                                        if loaded_caption:
                                            caption_text = loaded_caption
                                except:
                                    pass
                            
                            res = await reupload_with_pool(
                                clients, target,
                                files_to_upload,
                                caption=caption_text,
                                timeout=DEFAULT_UPLOAD_TIMEOUT
                            )
                            
                            # Track upload for the group
                            if extract_first_msg_id(res) > 0:
                                sent_id = extract_first_msg_id(res)
                                live_status(f"✅ Uploaded {len(files_to_upload)} files as grouped post (ID: {sent_id})", "SUCCESS")
                                # Mark all files as uploaded
                                for item in files_to_upload:
                                    try:
                                        mark_downloaded(item["path"], progress_key)
                                        file_hash = str(abs(hash(item["path"])) % 1000000)
                                        mark_uploaded("downloaded", target.id, file_hash, sent_id)
                                    except Exception:
                                        pass
                            else:
                                live_status(f"Failed to upload {len(files_to_upload)} files", "ERROR")

            elif choice == "8":
                for c in clients:
                    await c.log_out()
                break

            else:
                print("Invalid choice.")

        except Exception as e:
            live_status(f"Session error: {e}", "ERROR")
            traceback.print_exc()

        await safe_sleep(0.3)

# ───────────────────────────────
# MAIN ENTRY
# ───────────────────────────────
async def main():
    ensure_dirs()
    
    # Initialize tag filters
    tag_filter = get_tag_filter()
    if tag_filter.enabled:
        live_status(f"Tag filtering ENABLED", "INFO")
        if tag_filter.include_tags:
            live_status(f"  ✓ Include tags: {', '.join(sorted(tag_filter.include_tags))}", "INFO")
        if tag_filter.exclude_tags:
            live_status(f"  ✓ Exclude tags: {', '.join(sorted(tag_filter.exclude_tags))}", "INFO")
    else:
        live_status("Tag filtering disabled (all content types will be processed)", "INFO")
    
    # Display upload progress summary
    try:
        upload_summary = get_upload_summary()
        if upload_summary['total_upload_records'] > 0:
            live_status(f"📤 Upload Progress: {upload_summary['successful_uploads']} successful, {upload_summary['failed_uploads']} failed (total: {upload_summary['total_upload_records']})", "INFO")
            if upload_summary['last_updated']:
                live_status(f"   Last updated: {upload_summary['last_updated']}", "INFO")
    except Exception as e:
        if False:  # debug only
            live_status(f"Failed to load upload summary: {e}", "WARN")
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _signal_handler)

    while not _stop:
        choice = input("""
🔥 Smart Telegram Uploader v4.34
1) Use existing account(s)
2) Add new account
3) Exit
Choose: """).strip()

        if choice == "1":
            accounts_json = os.environ.get('ACCOUNTS_JSON')
            if accounts_json:
                try:
                    # use our safe loader to tolerate unescaped backslashes
                    from core.utils import _safe_json_loads
                    accounts = _safe_json_loads(accounts_json)
                except json.JSONDecodeError as exc:
                    live_status(f"Invalid ACCOUNTS_JSON format: {exc}", "ERROR")
                    live_status("Make sure all backslashes are escaped (use \\\\), or use forward slashes.", "INFO")
                    continue
            else:
                accounts_file = BASE_DIR / "config" / "accounts.json"
                if not accounts_file.exists():
                    live_status("No accounts file found and ACCOUNTS_JSON not set.", "ERROR")
                    continue
                try:
                    with open(accounts_file, "r", encoding="utf-8") as f:
                        accounts = json.load(f)
                except json.JSONDecodeError as exc:
                    live_status(f"accounts.json is corrupt: {exc}", "ERROR")
                    live_status("Fix or delete the file before retrying.", "INFO")
                    continue
            if not accounts:
                live_status("No accounts found.", "ERROR")
                continue
            clients = []
            for acc in accounts:
                try:
                    client = await create_client(acc)
                    if client:
                        clients.append(client)
                except Exception as e:
                    msg = str(e).lower()
                    if "database is locked" in msg:
                        live_status(
                            "Failed to load account (session database is locked). "
                            "Make sure no other bot/Telethon instance is using the same session file, "
                            "or delete the corresponding \"-journal\"/\"-wal\" files and try again.",
                            "ERROR"
                        )
                    elif "unable to open database file" in msg:
                        live_status(
                            "Failed to load account (unable to open session file). "
                            "Verify the session path in accounts.json and ensure the directory exists and is writable.",
                            "ERROR"
                        )
                    else:
                        live_status(f"Failed to load account: {e}", "ERROR")
            if clients:
                try:
                    await interactive_session(clients)
                finally:
                    for c in clients:
                        await c.disconnect()

        elif choice == "2":
            phone = input("Phone number: ").strip()
            api_id = os.environ.get('API_ID')
            api_hash = os.environ.get('API_HASH')
            if not api_id or not api_hash:
                live_status("API_ID and API_HASH must be set in environment variables.", "ERROR")
                continue
            accounts_file = BASE_DIR / "config" / "accounts.json"
            accounts = []
            if accounts_file.exists():
                with open(accounts_file, "r") as f:
                    accounts = json.load(f)
            accounts.append({"phone": phone})
            with open(accounts_file, "w") as f:
                json.dump(accounts, f, indent=2)
            live_status("Account added.", "SUCCESS")

        elif choice == "3":
            live_status("Goodbye!", "INFO")
            sys.exit(0)

if __name__ == "__main__":
    asyncio.run(main())