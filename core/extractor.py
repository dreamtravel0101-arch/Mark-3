# ============================================================
# THUMBNAIL EXTRACTOR v4.2 (TELEGRAM BULLETPROOF EDITION)
# Async + Sync • Multi Keyframes • Smart Compression Engine
# JPEG Forced • Adaptive Recompress • Metadata Strip
# Black Frame Avoid • Corruption Safe • Fast Seek Optimized
# Duration Safe • Fallback Timestamp • FFmpeg Detection
# ============================================================

import asyncio
from pathlib import Path
import subprocess
import shutil

# import binary paths so we honour FFMPEG_DIR env override
from core.reupload_manager import FFMPEG_BIN, FFPROBE_BIN

# ───────────────────────────────
# CONFIGURATION
# ───────────────────────────────
THUMB_TIME = 3  # seconds
THUMB_QUALITY = 2
MULTI_KEYFRAMES = False
KEYFRAME_COUNT = 3
KEYFRAME_INTERVAL = 1

MAX_THUMB_SIZE = 200 * 1024
MAX_WIDTH = 320

STRIP_METADATA = True
SHARPEN = False
ULTRA_COMPRESSION = True
AVOID_BLACK_FRAMES = True

# ───────────────────────────────
# FFmpeg Detection
# ───────────────────────────────
def _ffmpeg_exists():
    # prefer configured binary; fallback to PATH lookup
    if FFMPEG_BIN and shutil.which(FFMPEG_BIN):
        return True
    return shutil.which("ffmpeg") is not None


# ───────────────────────────────
# SAFE EXECUTION WRAPPER
# ───────────────────────────────
def _run_ffmpeg(cmd: list) -> bool:
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        return result.returncode == 0
    except Exception:
        return False


async def _run_ffmpeg_async(cmd: list) -> bool:
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )
        await process.wait()
        return process.returncode == 0
    except Exception:
        return False


# ───────────────────────────────
# GET VIDEO DURATION
# ───────────────────────────────
def _get_video_duration(video_path: Path) -> float:
    try:
        cmd = [
            FFPROBE_BIN or "ffprobe",
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            str(video_path)
        ]
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            encoding='utf-8',
            errors='replace'
        )
        return float(result.stdout.strip())
    except Exception:
        return 0.0


# ───────────────────────────────
# VALIDATE THUMB
# ───────────────────────────────
def _validate_thumb(path: Path) -> bool:
    if not path.exists():
        return False
    if path.stat().st_size == 0:
        return False
    if path.stat().st_size > MAX_THUMB_SIZE:
        return False
    return True


# ───────────────────────────────
# SMART SIZE ENFORCER
# ───────────────────────────────
def _ensure_size_limit(path: Path) -> bool:
    if not path.exists():
        return False

    if path.stat().st_size <= MAX_THUMB_SIZE:
        return True

    quality_steps = [8, 12, 16, 20, 24, 28, 31]

    for quality in quality_steps:
        cmd = [
            FFMPEG_BIN or "ffmpeg",
            "-y",
            "-i", str(path),
            "-vf", f"scale={MAX_WIDTH}:-1",
            "-q:v", str(quality),
            "-pix_fmt", "yuvj420p"
        ]

        if STRIP_METADATA:
            cmd += ["-map_metadata", "-1"]

        cmd += [str(path)]

        if not _run_ffmpeg(cmd):
            continue

        if path.stat().st_size <= MAX_THUMB_SIZE:
            return True

    return False


# ───────────────────────────────
# BUILD FILTER CHAIN
# ───────────────────────────────
def _build_filter():
    filters = [f"scale={MAX_WIDTH}:-1"]

    if SHARPEN:
        filters.append("unsharp=5:5:1.0:5:5:0.0")

    if AVOID_BLACK_FRAMES:
        filters.append("eq=brightness=0.03")

    return ",".join(filters)


# ───────────────────────────────
# BUILD FFMPEG CMD
# ───────────────────────────────
def _build_ffmpeg_cmd(video_path: Path, thumb_path: Path, timestamp: int):
    cmd = [
        FFMPEG_BIN or "ffmpeg",
        "-y",
        "-ss", str(timestamp),
        "-i", str(video_path),
        "-vframes", "1",
        "-vf", _build_filter(),
        "-q:v", str(THUMB_QUALITY),
        "-pix_fmt", "yuvj420p",
        "-an"
    ]

    if STRIP_METADATA:
        cmd += ["-map_metadata", "-1"]

    cmd += [str(thumb_path.with_suffix(".jpg"))]

    return cmd


# ───────────────────────────────
# SAFE TIMESTAMP
# ───────────────────────────────
def _safe_timestamp(video_path: Path) -> int:
    duration = _get_video_duration(video_path)
    if duration <= 0:
        return THUMB_TIME

    safe_time = min(THUMB_TIME, int(duration * 0.3))
    return max(1, safe_time)


# ───────────────────────────────
# ASYNC SINGLE THUMB
# ───────────────────────────────
async def generate_thumbnail(video_path: str, thumb_path: str, overwrite: bool = True) -> bool:
    if not _ffmpeg_exists():
        return False

    video_path = Path(video_path)
    thumb_path = Path(thumb_path).with_suffix(".jpg")

    if not video_path.exists():
        return False

    if thumb_path.exists() and not overwrite:
        return True

    thumb_path.parent.mkdir(parents=True, exist_ok=True)

    timestamp = _safe_timestamp(video_path)
    cmd = _build_ffmpeg_cmd(video_path, thumb_path, timestamp)

    if not await _run_ffmpeg_async(cmd):
        return False

    if not _validate_thumb(thumb_path):
        return False

    return _ensure_size_limit(thumb_path)


# ───────────────────────────────
# SYNC SINGLE THUMB
# ───────────────────────────────
def generate_thumbnail_sync(video_path: str, thumb_path: str, overwrite: bool = True) -> bool:
    if not _ffmpeg_exists():
        return False

    try:
        video_path = Path(video_path)
        thumb_path = Path(thumb_path).with_suffix(".jpg")

        if not video_path.exists():
            return False

        if thumb_path.exists() and not overwrite:
            return True

        thumb_path.parent.mkdir(parents=True, exist_ok=True)

        timestamp = _safe_timestamp(video_path)
        cmd = _build_ffmpeg_cmd(video_path, thumb_path, timestamp)

        if not _run_ffmpeg(cmd):
            return False

        if not _validate_thumb(thumb_path):
            return False

        return _ensure_size_limit(thumb_path)

    except Exception:
        return False


# ───────────────────────────────
# ASYNC MULTI KEYFRAMES
# ───────────────────────────────
async def generate_multiple_keyframes(video_path: str, folder: str, overwrite: bool = True) -> list:
    if not MULTI_KEYFRAMES:
        return []

    video_path = Path(video_path)
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)

    keyframes = []
    duration = _get_video_duration(video_path)

    for i in range(KEYFRAME_COUNT):
        timestamp = min(
            int(THUMB_TIME + i * KEYFRAME_INTERVAL),
            int(duration - 1) if duration > 1 else THUMB_TIME
        )

        keyframe_path = folder / f"keyframe_{i+1}.jpg"

        if keyframe_path.exists() and not overwrite:
            keyframes.append(str(keyframe_path))
            continue

        cmd = _build_ffmpeg_cmd(video_path, keyframe_path, timestamp)

        if await _run_ffmpeg_async(cmd):
            if _validate_thumb(keyframe_path) and _ensure_size_limit(keyframe_path):
                keyframes.append(str(keyframe_path))

    return keyframes


# ───────────────────────────────
# SYNC MULTI KEYFRAMES
# ───────────────────────────────
def generate_multiple_keyframes_sync(video_path: str, folder: str, overwrite: bool = True) -> list:
    if not MULTI_KEYFRAMES:
        return []

    video_path = Path(video_path)
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)

    keyframes = []
    duration = _get_video_duration(video_path)

    for i in range(KEYFRAME_COUNT):
        timestamp = min(
            int(THUMB_TIME + i * KEYFRAME_INTERVAL),
            int(duration - 1) if duration > 1 else THUMB_TIME
        )

        keyframe_path = folder / f"keyframe_{i+1}.jpg"

        if keyframe_path.exists() and not overwrite:
            keyframes.append(str(keyframe_path))
            continue

        cmd = _build_ffmpeg_cmd(video_path, keyframe_path, timestamp)

        if _run_ffmpeg(cmd):
            if _validate_thumb(keyframe_path) and _ensure_size_limit(keyframe_path):
                keyframes.append(str(keyframe_path))

    return keyframes