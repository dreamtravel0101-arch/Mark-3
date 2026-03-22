# ============================================================
# SMART TELEGRAM REUPLOAD ENGINE v10.1 (ULTIMATE MILITARY CORE)
# TRUE Drop-In Replacement • Zero Regression • Fully Hardened
# Atomic Album • Corruption Recovery • Smart Retry Engine
# Permission Safe • Upload Verification • Bulletproof Thumb
# ============================================================

import os
import asyncio
import random
import traceback
import json
import subprocess
import mimetypes
import time
from typing import List, Optional, Union, Callable, Awaitable

from telethon.errors import (
    FloodWaitError,
    ChatWriteForbiddenError,
    UserBannedInChannelError,
    RPCError,
)
from telethon.tl.types import (
    DocumentAttributeVideo,
    InputMediaUploadedPhoto,
    InputMediaUploadedDocument,
    InputReplyToMessage,
    InputPeerChannel,
    Document,
)
from telethon.tl import functions

try:
    from core.progress_bar import SimpleProgress
except Exception:
    SimpleProgress = None


# ───────────────── FORUM TOPIC ROUTING (RAW MTProto API) ─────────────────
async def send_file_with_forum_routing(client, target, file_path, caption="", reply_to=None,
                                       thumb=None, attributes=None, supports_streaming=False,
                                       silent=False, progress_callback=None):
    """
    Send file to forum topic using raw MTProto API.
    This bypasses Telethon's validation to properly support forum topic routing.
    
    Args:
        reply_to: Topic root message ID (will route to that topic)
    """
    print(f"[FORUM_UPLOAD] Using raw MTProto API for forum topic routing (reply_to={reply_to})")
    
    # Build the reply_to object with BOTH fields required for forum routing
    reply_to_obj = None
    if reply_to and isinstance(reply_to, int):
        reply_to_obj = InputReplyToMessage(
            reply_to_msg_id=reply_to,  # Topic root message ID
            top_msg_id=reply_to         # Critical: Also set topic ID
        )
        print(f"[FORUM_UPLOAD] Built InputReplyToMessage: reply_to_msg_id={reply_to}, top_msg_id={reply_to}")
    
    # Upload file through normal channel first (gets media object)
    uploaded_media = await client.upload_file(file_path, progress_callback=progress_callback)
    
    # Now send using raw API with proper forum routing
    try:
        # Get peer entity
        peer = await client.get_input_entity(target)
        
        # Build the media upload object
        media = None
        if isinstance(uploaded_media, Document):
            media = uploaded_media
        else:
            # It's a file - wrap it properly
            media = InputMediaUploadedDocument(
                file=uploaded_media,
                mime_type=guess_mime(file_path),
                attributes=attributes or [],
                thumb=None  # Thumb handled separately by Telethon
            )
        
        # Use raw SendMediaRequest with forum routing
        request = functions.messages.SendMediaRequest(
            peer=peer,
            media=media,
            message=caption or "",
            reply_to=reply_to_obj,  # This is the key - raw API accepts InputReplyToMessage!
            silent=silent,
        )
        
        result = await client(request)
        print(f"[FORUM_UPLOAD] ✅ Sent via raw MTProto API - {result}")
        return result
        
    except Exception as e:
        print(f"[FORUM_UPLOAD] Raw API attempt failed: {e}")
        print(f"[FORUM_UPLOAD] Falling back to standard send_file()...")
        # Fallback to standard method
        return await client.send_file(
            target,
            file_path,
            caption=caption,
            thumb=thumb,
            attributes=attributes,
            supports_streaming=supports_streaming,
            silent=silent,
        )


# ───────────────── CONFIG ─────────────────
DEFAULT_TIMEOUT = 3600  # 1 hour timeout for retry logic
MAX_TIMEOUT = 518400
MIN_DELAY = 0  # Set to 0 for maximum speed - no artificial delays
MAX_DELAY = 0   # Set to 0 for maximum speed - no artificial delays
INTER_UPLOAD_DELAY = 0.1  # Minimal spacing between album uploads (configurable via env var)

# Allow overriding upload pacing via environment variables for speed tuning
try:
    env_min = os.getenv("MIN_UPLOAD_DELAY")
    env_max = os.getenv("MAX_UPLOAD_DELAY")
    env_factor = os.getenv("UPLOAD_DELAY_FACTOR")
    env_inter = os.getenv("INTER_UPLOAD_DELAY")
    if env_min:
        MIN_DELAY = float(env_min)
    if env_max:
        MAX_DELAY = float(env_max)
    if env_factor:
        _global_delay_factor = float(env_factor)
    if env_inter:
        INTER_UPLOAD_DELAY = float(env_inter)
except Exception:
    pass
MAX_RETRIES = 3  # Conservative - avoid hammering on failures
MAX_FLOODWAIT = 7200  # BULLETPROOF: 2 hour max buffer
MAX_THUMB_SIZE = 200 * 1024
TELEGRAM_ALBUM_LIMIT = 10
CONNECTION_RETRY_WAIT = 10  # BULLETPROOF: Longer spacing between retries
UPLOAD_CONCURRENCY = 1  # BULLETPROOF: Serial uploads only - no parallel requests

# Output resolution limits (downscale 4K/8K to max 1080p/720p)
MAX_VIDEO_WIDTH = 1920  # 1080p default (3840 for 4K, 1280 for 720p)
MAX_VIDEO_HEIGHT = 1080  # 1080p default (2160 for 4K, 720 for 720p)
try:
    env_res = os.getenv("MAX_OUTPUT_RESOLUTION")  # Set to "720p" to use 720p limit
    if env_res and env_res.lower() == "720p":
        MAX_VIDEO_WIDTH = 1280
        MAX_VIDEO_HEIGHT = 720
except Exception:
    pass

_global_delay_factor = 1.0
_ffmpeg_checked = False

FFMPEG_DIR = r"D:\ffmpeg\bin"
FFMPEG_BIN = os.path.join(FFMPEG_DIR, "ffmpeg.exe")
FFPROBE_BIN = os.path.join(FFMPEG_DIR, "ffprobe.exe")


# ───────────────── SYSTEM CHECK ─────────────────
def ensure_ffmpeg():
    global _ffmpeg_checked
    if _ffmpeg_checked:
        return

    if not (os.path.isfile(FFMPEG_BIN) and os.path.isfile(FFPROBE_BIN)):
        raise RuntimeError("FFmpeg / FFprobe not found")

    _ffmpeg_checked = True


# ───────────────── NORMALIZATION ─────────────────
def normalize_inputs(paths: List[Union[str, dict]]):
    out = []
    for item in paths:
        if isinstance(item, dict):
            out.append({
                "path": item.get("path"),
                "thumb": item.get("thumb"),
            })
        else:
            out.append({"path": item, "thumb": None})
    return out


# ───────────────── VALIDATION ─────────────────
def validate_file(p: str) -> bool:
    return (
        isinstance(p, str)
        and os.path.isfile(p)
        and os.path.getsize(p) > 0
    )


def validate_thumb(p: Optional[str]) -> bool:
    return (
        isinstance(p, str)
        and os.path.isfile(p)
        and p.lower().endswith(".jpg")
        and 0 < os.path.getsize(p) <= MAX_THUMB_SIZE
    )


# ───────────────── THUMB GENERATION ─────────────────
def generate_video_thumb(path: str) -> Optional[str]:
    try:
        ensure_ffmpeg()
        thumb_path = f"{path}.thumb_{int(time.time())}.jpg"

        result = subprocess.run(
            [
                FFMPEG_BIN,
                "-y",
                "-i", path,
                "-ss", "00:00:01.000",
                "-vframes", "1",
                "-vf", "scale=320:-1",
                thumb_path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            errors='replace',
            check=False,
        )

        if result.returncode != 0 and result.stderr:
            print(f"[THUMB GEN] ⚠ Failed to generate thumbnail: {result.stderr[:200] if result.stderr else 'error'}")

        return thumb_path if validate_thumb(thumb_path) else None
    except Exception as e:
        print(f"[THUMB GEN] ⚠ Exception: {e}")
        return None


# ───────────────── METADATA ─────────────────
def extract_video_metadata(path: str):
    try:
        ensure_ffmpeg()
        r = subprocess.run(
            [
                FFPROBE_BIN,
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=width,height",
                "-show_entries", "format=duration",
                "-of", "json",
                path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=20,
            text=True,
            encoding='utf-8',
            errors='replace',
            check=False,
        )

        if r.returncode != 0:
            print(f"[VIDEO METADATA] ✗ ffprobe failed: {r.stderr[:200] if r.stderr else 'unknown'}")
            return None

        data = json.loads(r.stdout if r.stdout else "{}")
        streams = data.get("streams") or [{}]
        fmt = data.get("format") or {}

        w = int(streams[0].get("width", 0))
        h = int(streams[0].get("height", 0))
        d = int(float(fmt.get("duration", 0)))

        if w > 0 and h > 0 and d > 0:
            return w, h, d
        else:
            print(f"[VIDEO METADATA] ✗ Invalid dimensions: {w}x{h}, duration: {d}s")
            return None
    except Exception as e:
        print(f"[VIDEO METADATA] ✗ Exception: {e}")
        return None


# ───────────────── VIDEO PREP ─────────────────
def telegram_prepare_video(path: str) -> Optional[str]:
    if not path.lower().endswith((".mp4", ".mkv", ".mov", ".ts", ".webm", ".avi")):
        return path

    # Check if resolution scaling is needed
    meta = extract_video_metadata(path)
    needs_scale = False
    if meta:
        w, h, d = meta
        max_dimension = max(w, h)
        # Only scale if longest dimension exceeds our max (1080p)
        # This preserves aspect ratio: portrait stays portrait, landscape stays landscape
        if max_dimension > MAX_VIDEO_WIDTH:
            needs_scale = True
            print(f"[VIDEO PREP] Source: {w}x{h}, longest dim {max_dimension}px > 1080p, will scale")

    # Try simple stream copy first (only if no scaling needed)
    if not needs_scale:
        fixed = f"{path}.tgfix_{int(time.time())}.mp4"
        try:
            ensure_ffmpeg()
            result = subprocess.run(
                [
                    FFMPEG_BIN,
                    "-y",
                    "-i", path,
                    "-map", "0:v:0",
                    "-map", "0:a?",
                    "-c", "copy",
                    "-movflags", "+faststart",
                    fixed,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding='utf-8',
                errors='replace',
                check=False,
            )

            if validate_file(fixed):
                return fixed
            elif result.returncode != 0:
                # Stream copy failed, will fall through to re-encode
                pass
        except Exception:
            pass

    # If stream copy fails or scaling needed, use re-encoding with scaling
    print(f"Using re-encode with resolution scaling for {path}...")
    reencoded = force_reencode(path)
    if reencoded:
        return reencoded

    return None




# ───────────────── CORRUPTION RECOVERY ─────────────────
def force_reencode(path: str) -> Optional[str]:
    try:
        ensure_ffmpeg()
        fixed = f"{path}.reencoded_{int(time.time())}.mp4"

        # Get video dimensions to preserve orientation
        meta = extract_video_metadata(path)
        if not meta:
            print(f"[VIDEO PREP] ✗ Could not extract video metadata")
            return None

        w, h, d = meta
        is_portrait = h > w
        
        # Scale filter: cap max dimension at 1080p while preserving aspect ratio
        # For portrait: scale height to 1080, width scales proportionally
        # For landscape: scale width to 1080, height scales proportionally
        if is_portrait:
            scale_filter = "scale=-2:1080"  # Height = 1080, width = auto
            print(f"[VIDEO PREP] Portrait video: {w}x{h} → height capped at 1080")
        else:
            scale_filter = "scale=1080:-2"  # Width = 1080, height = auto
            print(f"[VIDEO PREP] Landscape video: {w}x{h} → width capped at 1080")

        result = subprocess.run(
            [
                FFMPEG_BIN,
                "-y",
                "-i", path,
                "-c:v", "libx264",
                "-preset", "veryfast",
                "-pix_fmt", "yuv420p",
                "-vf", scale_filter,
                "-movflags", "+faststart",
                "-c:a", "aac",
                "-b:a", "128k",
                fixed,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            errors='replace',
            check=False,
        )

        # Check for errors
        if result.returncode != 0:
            error_msg = result.stderr[-500:] if result.stderr else "Unknown error"
            print(f"[VIDEO PREP] ✗ FFmpeg failed: {error_msg}")
            return None

        if not validate_file(fixed):
            print(f"[VIDEO PREP] ✗ Output file not created or empty")
            return None

        return fixed
    except Exception as e:
        print(f"[VIDEO PREP] ✗ Exception: {e}")
        return None


# ───────────────── PERMISSION CHECK ─────────────────
async def check_permissions(client, target):
    try:
        entity = await client.get_entity(target)

        if hasattr(entity, "broadcast") or hasattr(entity, "megagroup"):
            perms = await client.get_permissions(entity, client.me)
            if hasattr(perms, "send_messages") and not perms.send_messages:
                return False
    except (ChatWriteForbiddenError, UserBannedInChannelError):
        return False
    except Exception:
        pass

    return True


# ───────────────── SAFE EXEC ─────────────────
async def safe_exec(func: Callable[[], Awaitable], timeout):
    timeout = min(timeout, MAX_TIMEOUT)
    retry = 0
    backoff = 5

    while retry < MAX_RETRIES:
        try:
            return await asyncio.wait_for(func(), timeout)

        except FloodWaitError as e:
            await asyncio.sleep(min(e.seconds, MAX_FLOODWAIT))

        except (OSError, ConnectionError) as e:
            # Connection errors need longer waits
            error_code = getattr(e, 'winerror', getattr(e, 'errno', 0))
            print(f"🌐 Connection error (code {error_code}): {str(e)[:80]} → waiting {CONNECTION_RETRY_WAIT}s...")
            await asyncio.sleep(CONNECTION_RETRY_WAIT)

        except asyncio.TimeoutError:
            await asyncio.sleep(backoff)

        except (ChatWriteForbiddenError, UserBannedInChannelError):
            return None

        except RPCError as e:
            msg = str(e).lower()
            if any(x in msg for x in (
                "file parts invalid",
                "media invalid",
                "video content type invalid",
                "media empty",
            )):
                return None
            await asyncio.sleep(backoff)

        except RuntimeError:
            # Propagate cancellation / explicit runtime errors immediately
            raise
        except Exception:
            traceback.print_exc()
            await asyncio.sleep(backoff)

        retry += 1
        backoff = min(backoff * 1.7, 120)

    return None


# ───────────────── MIME ─────────────────
def guess_mime(path: str):
    return mimetypes.guess_type(path)[0] or "application/octet-stream"


# ───────────────── UPLOAD CORE ─────────────────
async def upload_post(client, target, items, caption, reply_to, silent, timeout, progress, cancel_event=None):
    if not await check_permissions(client, target):
        return None

    # Convert reply_to integer to proper InputReplyToMessage for forum topics
    # CRITICAL: Both reply_to_msg_id AND top_msg_id must be set for forum routing to work!
    if reply_to and isinstance(reply_to, int):
        reply_to = InputReplyToMessage(
            reply_to_msg_id=reply_to,
            top_msg_id=reply_to
        )

    upload_start_time = __import__('time').time()
    prepared = []  # Initialize prepared list
    for entry in items:
        p = entry["path"]
        if not validate_file(p):
            print(f"[UPLOAD] ✗ Invalid source file: {p}")
            continue

        new_path = telegram_prepare_video(p)
        if new_path is None:
            print(f"[UPLOAD] ✗ Video prep failed for {p}")
            continue
        
        # Validate the prepared file exists and has content
        if not validate_file(new_path):
            print(f"[UPLOAD] ✗ Prepared file invalid or empty: {new_path} ({__import__('os').path.getsize(new_path) if __import__('os').path.exists(new_path) else 'missing'} bytes)")
            continue
            
        print(f"[UPLOAD] ✓ Prepared file ready: {__import__('os').path.basename(new_path)} ({__import__('os').path.getsize(new_path) / 1024 / 1024:.1f}MB)")
        
        thumb = entry.get("thumb")

        if new_path.lower().endswith((".mp4", ".mkv", ".mov", ".ts", ".webm", ".avi")):
            if not validate_thumb(thumb):
                thumb = generate_video_thumb(new_path)

        prepared.append({"path": new_path, "thumb": thumb})

    if not prepared:
        print(f"[UPLOAD] ✗ No videos prepared successfully - upload will fail")
        return None

    # Display caption/tags before upload starts (like we do in download)
    if caption:
        caption_text = caption.strip()
        if caption_text:
            display_caption = caption_text[:80]  # Show up to 80 chars
            if len(caption_text) > 80:
                display_caption += "..."
            print(f"⬆️ {display_caption}")

    stickers = [x for x in prepared if x["path"].lower().endswith((".webp", ".tgs"))]
    media = [x for x in prepared if x not in stickers]

    # Display upload composition summary
    video_count = sum(1 for x in media if x["path"].lower().endswith((".mp4", ".mkv", ".mov", ".ts", ".webm", ".avi")))
    photo_count = sum(1 for x in media if x["path"].lower().endswith((".jpg", ".jpeg", ".png", ".gif", ".webp")))
    doc_count = sum(1 for x in media if x["path"].lower().endswith((".pdf", ".doc", ".docx", ".txt", ".zip")))
    total_size_bytes = sum(__import__('os').path.getsize(x["path"]) for x in media if __import__('os').path.exists(x["path"]))
    total_size_mb = total_size_bytes / 1024 / 1024
    
    print(f"[UPLOAD_QUEUE] 📤 {len(media)} items: {video_count} video(s), {photo_count} photo(s), {doc_count} doc(s) | Total: {total_size_mb:.1f}MB")

    uploaded_bytes = 0

    def make_cb():
        last = 0

        def cb(cur, total=None):
            nonlocal last, uploaded_bytes
            # Allow cancellation from the callback
            if cancel_event is not None and getattr(cancel_event, "is_set", lambda: False)():
                raise RuntimeError("Upload cancelled by user")
            delta = cur - last
            last = cur
            if delta > 0:
                uploaded_bytes += delta
            if progress:
                progress.update(uploaded_bytes)

        return cb

    # ───── SINGLE ─────
    if len(media) == 1:
        item = media[0]
        p = item["path"]
        thumb = item["thumb"] if validate_thumb(item["thumb"]) else None

        attrs = []
        if p.lower().endswith((".mp4", ".mkv", ".mov", ".ts", ".webm", ".avi")):
            meta = extract_video_metadata(p)
            if meta:
                w, h, d = meta
                attrs = [DocumentAttributeVideo(d, w, h, supports_streaming=True)]

        try:
            # Use raw MTProto API for forum topic routing if reply_to is set
            if reply_to:
                result = await safe_exec(
                    lambda: send_file_with_forum_routing(
                        client,
                        target,
                        p,
                        caption=caption,
                        reply_to=reply_to,
                        thumb=thumb,
                        attributes=attrs,
                        supports_streaming=True,
                        silent=silent,
                        progress_callback=make_cb(),
                    ),
                    timeout,
                )
            else:
                result = await safe_exec(
                    lambda: client.send_file(
                        target,
                        p,
                        caption=caption,
                        thumb=thumb,
                        attributes=attrs,
                        supports_streaming=True,
                        reply_to=reply_to,
                        silent=silent,
                        progress_callback=make_cb(),
                    ),
                    timeout,
                )
            if result:
                print(f"[UPLOAD] ✓ Single upload succeeded, message id: {result.id if hasattr(result, 'id') else 'unknown'}")
            else:
                print(f"[UPLOAD] ✗ Single upload returned None, retrying with re-encode...")
        except RuntimeError as e:
            # cancellation requested
            print(f"[UPLOAD] ✗ Upload cancelled: {e}")
            raise
        except Exception as e:
            print(f"[UPLOAD] ✗ Upload failed: {e}")
            result = None

        if not result and p.lower().endswith((".mp4", ".mkv", ".mov", ".ts", ".webm", ".avi")):
            fixed = force_reencode(p)
            if fixed:
                print(f"[UPLOAD] 🔄 Re-encoded file: {__import__('os').path.basename(fixed)} ({__import__('os').path.getsize(fixed) / 1024 / 1024:.1f}MB)")
                if reply_to:
                    result = await safe_exec(
                        lambda: send_file_with_forum_routing(
                            client,
                            target,
                            fixed,
                            caption=caption,
                            reply_to=reply_to,
                            supports_streaming=True,
                            silent=silent,
                        ),
                        timeout,
                    )
                else:
                    result = await safe_exec(
                        lambda: client.send_file(
                            target,
                            fixed,
                            caption=caption,
                            supports_streaming=True,
                            reply_to=reply_to,
                            silent=silent,
                        ),
                        timeout,
                    )
        upload_end_time = __import__('time').time()
        upload_duration = upload_end_time - upload_start_time
        return result

    # ───── ALBUM ─────
    sent_objects = []
    caption_applied = False  # Track if caption has been applied to any message
    
    for i in range(0, len(media), TELEGRAM_ALBUM_LIMIT):
        chunk = media[i:i + TELEGRAM_ALBUM_LIMIT]
        album = []
        # Upload files for this album chunk. To improve throughput we upload
        # files concurrently (configurable via UPLOAD_FILE_CONCURRENCY env var)
        # Default to 2 for concurrent uploads (optimal for 2-account setups)
        concurrency = int(os.getenv("UPLOAD_FILE_CONCURRENCY", "2"))
        sem = asyncio.Semaphore(concurrency)

        async def _upload_file(item):
            p = item["path"]
            thumb = item.get("thumb")
            try:
                async with sem:
                    file = await safe_exec(
                        lambda: client.upload_file(p, progress_callback=make_cb()),
                        timeout,
                    )
            except RuntimeError:
                raise
            except Exception:
                return None

            if not file:
                return None

            thumb_file = None
            if validate_thumb(thumb):
                try:
                    thumb_file = await safe_exec(lambda: client.upload_file(thumb), timeout)
                except Exception:
                    thumb_file = None

            attrs = []
            if p.lower().endswith((".mp4", ".mkv", ".mov", ".ts", ".webm", ".avi")):
                meta = extract_video_metadata(p)
                if meta:
                    w, h, d = meta
                    attrs = [DocumentAttributeVideo(d, w, h, supports_streaming=True)]

            return (p, file, thumb_file, attrs)

        upload_tasks = [asyncio.create_task(_upload_file(item)) for item in chunk]
        uploaded_results = await asyncio.gather(*upload_tasks)

        for res in uploaded_results:
            if not res:
                continue
            p, file, thumb_file, attrs = res
            if p.lower().endswith((".jpg", ".png")):
                album.append(InputMediaUploadedPhoto(file))
            else:
                album.append(
                    InputMediaUploadedDocument(
                        file=file,
                        mime_type=guess_mime(p),
                        attributes=attrs,
                        thumb=thumb_file,
                    )
                )

        if not album:
            continue

        sent = await safe_exec(
            lambda: client.send_file(
                target,
                album,
                caption=caption if i == 0 else None,
                reply_to=reply_to,
                silent=silent,
            ),
            timeout,
        )

        if sent:
            # collect sent message(s)
            print(f"[UPLOAD] ✓ Album chunk {i//TELEGRAM_ALBUM_LIMIT + 1} uploaded: {len(sent) if isinstance(sent, list) else 1} items")
            sent_objects.append(sent)
            if i == 0 and caption:
                caption_applied = True
        else:
            # fallback: send items individually and collect results
            print(f"[UPLOAD] ⚠ Album chunk failed, sending items individually...")
            # Apply caption only to the first item sent across ALL chunks
            for idx, item in enumerate(chunk):
                p = item["path"]
                thumb = item.get("thumb")
                
                # Set video attributes if it's a video file
                attrs = []
                if p.lower().endswith((".mp4", ".mkv", ".mov", ".ts", ".webm", ".avi")):
                    meta = extract_video_metadata(p)
                    if meta:
                        w, h, d = meta
                        attrs = [DocumentAttributeVideo(d, w, h, supports_streaming=True)]
                
                # Apply caption only to the first item being sent (across all chunks)
                should_add_caption = caption and not caption_applied and idx == 0 and i == 0
                
                res = await safe_exec(
                    lambda item=item, idx=idx, thumb=thumb, attrs=attrs: client.send_file(
                        target,
                        item["path"],
                        caption=caption if should_add_caption else None,
                        thumb=thumb,
                        attributes=attrs,
                        reply_to=reply_to,
                        silent=silent,
                        supports_streaming=True,
                    ),
                    timeout,
                )
                if res:
                    sent_objects.append(res)
                    if should_add_caption:
                        caption_applied = True

        await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY) * _global_delay_factor)
        # BULLETPROOF: Add mandatory spacing between album uploads to avoid flood wait
        await asyncio.sleep(INTER_UPLOAD_DELAY)

    # Send stickers with caption applied to first sticker if caption wasn't applied yet
    for s_idx, s in enumerate(stickers):
        should_add_sticker_caption = caption and not caption_applied and s_idx == 0
        
        res = await safe_exec(
            lambda s=s: client.send_file(
                target, 
                s["path"], 
                caption=caption if should_add_sticker_caption else None,
                silent=silent
            ),
            timeout,
        )
        if res:
            sent_objects.append(res)
            if should_add_sticker_caption:
                caption_applied = True

    # Return collected sent objects when available, else True to indicate success
    upload_end_time = __import__('time').time()
    upload_duration = upload_end_time - upload_start_time
    
    if not sent_objects:
        return True
    if len(sent_objects) == 1:
        return sent_objects[0]
    return sent_objects


# ───────────────── ENGINE ─────────────────
async def reupload_with_pool(
    clients,
    target,
    paths,
    caption="",
    timeout=DEFAULT_TIMEOUT,
    reply_to=None,
    silent=False,
    cancel_event=None,
):
    if not clients:
        return None

    normalized = normalize_inputs(paths)

    total = sum(
        os.path.getsize(x["path"])
        for x in normalized
        if validate_file(x["path"])
    )

    progress = None
    if SimpleProgress and total > 0:
        # build prefix without caption (shown separately above progress bar)
        def _prefix_label(caption_text):
            return "⬆️ Uploading"

        progress = SimpleProgress(total, prefix=_prefix_label(caption))
        progress.set_stage("Uploading")
        result = await upload_post(
            clients[0], target, normalized, caption, reply_to, silent, timeout, progress, cancel_event=cancel_event
        )
    else:
        result = await upload_post(
            clients[0], target, normalized, caption, reply_to, silent, timeout, None, cancel_event=cancel_event
        )

    if progress:
        progress.done()

    return result


# ───────────────── WRAPPER ─────────────────
class Uploader:
    def __init__(self, clients):
        self.clients = clients

    async def reupload(self, *args, **kwargs):
        return await reupload_with_pool(self.clients, *args, **kwargs)