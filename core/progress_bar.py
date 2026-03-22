"""
Advanced progress bar utilities for the bot.

Provides:
- `SimpleProgress`: thread-safe progress bar compatible with Telethon callbacks
- `show_progress_line`: (optional) yt-dlp stdout line parser for a lively text bar
- summary helpers for downloads

This module is intentionally self-contained and safe to import from any part of the bot.
"""

import sys
import time
import threading
import shutil
import re
import itertools
from typing import Union
from pathlib import Path


def _display_width(text: str) -> int:
    """Calculate the display width of text, accounting for multi-byte characters.
    
    CJK characters (Chinese, Japanese, Korean) take 2 columns in terminal display.
    Most other characters take 1 column.
    """
    width = 0
    for char in text:
        # CJK Unified Ideographs and other wide character ranges
        if ord(char) >= 0x2E80:  # CJK and beyond
            width += 2
        elif char in '\t':  # Tab = 8 spaces (simplified)
            width += 8
        elif ord(char) < 0x20:  # Control characters
            width += 0
        else:
            width += 1
    return width


class SimpleProgress:
    """
    Advanced single-line progress bar for upload/download tasks.
    Compatible with Telethon progress callbacks (current, total).
    """

    def __init__(self, total_items: int = 1, prefix: str = "", file_path: str = None, album_mode: bool = False):
        if file_path:
            file_path = Path(file_path)
            if file_path.exists() and file_path.is_file():
                total_items = file_path.stat().st_size

        self.total = max(int(total_items), 1)
        self.current = 0
        self.prefix = prefix or "Progress"
        self.stage = ""
        self.start_time = time.time()
        self.bar_len = 30
        self.album_mode = album_mode

        self._lock = threading.Lock()
        self._last_update_time = self.start_time
        self._last_bytes = 0
        self._speed_avg = 0.0
        self.is_tty = sys.stdout.isatty()
        self.done_flag = False

        self._item_idx = 0
        self._total_items = total_items

    @staticmethod
    def _format_size(size_bytes: float) -> str:
        size_bytes = max(size_bytes, 0)
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024:
                return f"{size_bytes:.2f}{unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f}PB"

    @staticmethod
    def _format_time(seconds: float) -> str:
        """Format seconds as HH:MM:SS"""
        seconds = max(0, int(seconds))
        h = seconds // 3600
        m = (seconds % 3600) // 60
        s = seconds % 60
        return f"{h:02d}:{m:02d}:{s:02d}"

    def _get_terminal_width(self):
        try:
            return shutil.get_terminal_size().columns
        except Exception:
            return 120

    def _safe_write(self, text: str):
        # Always use carriage return for single-line output
        width = self._get_terminal_width()
        
        # Truncate text to fit terminal width, accounting for multi-byte characters
        display_width = _display_width(text)
        if display_width > width - 1:
            # Need to truncate, but carefully with multi-byte chars
            truncated = ""
            for char in text:
                if _display_width(truncated + char) >= width - 1:
                    break
                truncated += char
        else:
            truncated = text
        
        # Pad to width and use carriage return to overwrite same line (no newline)
        padding = width - _display_width(truncated)
        sys.stdout.write(f"\r{truncated}{' ' * padding}")
        sys.stdout.flush()

    def set_stage(self, stage_name: str):
        with self._lock:
            self.stage = stage_name
            self.start_time = time.time()
            self._last_update_time = self.start_time
            self._last_bytes = 0
            self.current = 0
            self._speed_avg = 0.0
            self.done_flag = False
            self._item_idx = 0
            # Clear line before new stage
            width = self._get_terminal_width()
            sys.stdout.write(f"\r{' ' * width}\r")
            sys.stdout.flush()

    def update(self, current_bytes: int = None, total_bytes: int = None,
               item_idx: int = None, total_items: int = None):
        if self.done_flag:
            return

        with self._lock:
            now = time.time()

            if total_bytes is not None and total_bytes > 0:
                self.total = max(int(total_bytes), 1)

            if current_bytes is not None:
                self.current = max(0, min(float(current_bytes), float(self.total)))

            if item_idx is not None:
                self._item_idx = item_idx
            if total_items is not None:
                self._total_items = total_items

            percent = (self.current / self.total) * 100
            percent = min(percent, 100.0)

            elapsed = now - self.start_time
            delta_time = max(now - self._last_update_time, 0.05)
            delta_bytes = max(self.current - self._last_bytes, 0)
            instant_speed = delta_bytes / delta_time

            self._speed_avg = self._speed_avg * 0.85 + instant_speed * 0.15 if self._speed_avg else instant_speed

            self._last_bytes = self.current
            self._last_update_time = now

            remaining = (self.total - self.current) / self._speed_avg if self._speed_avg > 0 else 0
            speed_str = self._format_size(self._speed_avg) + "/s"
            elapsed_str = self._format_time(elapsed)
            remaining_str = self._format_time(remaining)

            filled_len = int(self.bar_len * percent / 100)
            bar = "█" * filled_len + "-" * (self.bar_len - filled_len)

            item_info = ""
            if self.album_mode and self._total_items > 1:
                item_info = f"({self._item_idx}/{self._total_items}) "

            line = (
                f"{self.prefix} | {self.stage} {item_info}| "
                f"[{bar}] {percent:6.2f}% | "
                f"{self._format_size(self.current)}/{self._format_size(self.total)} | "
                f"{speed_str} | ETA: {remaining_str} | Elapsed: {elapsed_str}"
            )

            self._safe_write(line)

    def done(self):
        with self._lock:
            if self.done_flag:
                return
            self.done_flag = True
            total_time = time.time() - self.start_time
            self.current = self.total
            bar = "█" * self.bar_len
            final_line = (
                f"{self.prefix} | {self.stage} ✅ Done | "
                f"[{bar}] 100.00% | {self._format_size(self.total)} | "
                f"Total Time: {self._format_time(total_time)}"
            )
            # Always clear line and print with newline at end
            width = self._get_terminal_width()
            padding = max(0, width - _display_width(final_line))
            sys.stdout.write(f"\r{final_line}{' ' * padding}\n")
            sys.stdout.flush()

    def callback(self):
        return lambda current, total=None: self.update(current, total)


# ================= EMOJIS & SPARKLES (yt-dlp helper) =================

EMOJIS = {
    10: "✨",
    20: "🔥",
    30: "🌑",
    40: "🌒",
    50: "🌓",
    60: "🌔",
    70: "🌕",
    80: "🚀",
    90: "🏁",
    100: "✅",
}

SPARKLE = itertools.cycle(["✦", "✧", "✩", "✪", "★"])


def format_time(seconds: Union[int, float]) -> str:
    seconds = max(0, int(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02}:{m:02}:{s:02}"


def format_size(bytes_: float) -> str:
    if bytes_ < 1024:
        return f"{bytes_:.0f} B"
    elif bytes_ < 1024 ** 2:
        return f"{bytes_ / 1024:.1f} KB"
    elif bytes_ < 1024 ** 3:
        return f"{bytes_ / (1024 ** 2):.1f} MB"
    return f"{bytes_ / (1024 ** 3):.2f} GB"


def parse_unit_size(value: str, unit: str) -> float:
    unit = unit.upper()
    factor = {
        "KIB": 1024,
        "MIB": 1024 ** 2,
        "GIB": 1024 ** 3,
        "KB": 1000,
        "MB": 1000 ** 2,
        "GB": 1000 ** 3,
    }.get(unit, 1)
    return float(value) * factor


def show_progress_line(line: str, max_perc: float) -> float:
    """Parse yt-dlp output line and render a lively text progress bar.
    Returns updated max percentage.
    """
    if not line.startswith("[download]"):
        print(line)
        return max_perc

    perc_match = re.search(r"(\d+(?:\.\d+)?)%", line)
    if not perc_match:
        return max_perc

    perc = float(perc_match.group(1))
    max_perc = max(max_perc, perc)

    emoji = ""
    for m in sorted(EMOJIS):
        if max_perc >= m:
            emoji = EMOJIS[m]

    bar_len = 40
    filled = int(bar_len * max_perc / 100)
    bar = "█" * filled + next(SPARKLE) + "-" * max(bar_len - filled - 1, 0)

    info_parts = []

    size_match = re.search(
        r"([\d.]+)\s*(KiB|MiB|GiB|KB|MB|GB)\s+of\s+([\d.]+)\s*(KiB|MiB|GiB|KB|MB|GB)",
        line,
    )
    if size_match:
        cur_v, cur_u, tot_v, tot_u = size_match.groups()
        cur_bytes = parse_unit_size(cur_v, cur_u)
        tot_bytes = parse_unit_size(tot_v, tot_u)
        info_parts.append(f"{format_size(cur_bytes)} / {format_size(tot_bytes)}")

    speed_match = re.search(r"at\s+([\d.]+\s*(?:KiB|MiB|GiB|KB|MB|GB)/s)", line)
    if speed_match:
        info_parts.append(f"⚡ {speed_match.group(1)}")

    eta_match = re.search(r"ETA\s+([\d:]+)", line)
    if eta_match:
        info_parts.append(f"⏳ {eta_match.group(1)}")

    info = " | " + " | ".join(info_parts) if info_parts else ""

    print(f"[{bar}] {max_perc:5.1f}% {emoji}{info}", end="\r", flush=True)
    return max_perc


def print_video_complete(downloaded: int, total: int, skipped: int) -> None:
    remaining = total - downloaded - skipped
    print(
        f"\n✅ Downloaded: {downloaded}/{total} | "
        f"⏳ Remaining: {remaining} | ❌ Skipped: {skipped}\n"
    )


def print_final_summary(downloaded: int, total: int, skipped: int) -> None:
    remaining = total - downloaded - skipped
    print("\n📊 FINAL SUMMARY")
    print(f"✅ Downloaded: {downloaded}/{total}")
    print(f"⏳ Remaining: {remaining}")
    print(f"❌ Skipped: {skipped}\n")