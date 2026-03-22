# ============================================================
# CAPTION WITH LINKS - Parse Telegram links + Add Captions
# Integrates link parser with Bangla caption generator
# ============================================================

import re
import os
import cv2
import numpy as np
from pathlib import Path
from typing import List, Optional, Tuple
from datetime import datetime
import urllib.request

try:
    from PIL import Image, ImageDraw, ImageFont
except ImportError:
    Image = None
    ImageDraw = None
    ImageFont = None


# ───────────────────────────────
# CONSTANTS
# ───────────────────────────────
BASE_DIR = Path(__file__).parent.parent
FONTS_DIR = BASE_DIR / "fonts"
BANGLA_FONT_URL = "https://github.com/nuhil/fonts/raw/master/Bangla/SiyamRupali.ttf"
BANGLA_FONT_PATH = FONTS_DIR / "SiyamRupali.ttf"

CAPTION_POSITION = "top"  # or "bottom"
CAPTION_FONT_SIZE = 24
CAPTION_COLOR = (255, 255, 255)  # White in BGR for OpenCV
CAPTION_BG_COLOR = (0, 0, 0)  # Black background
CAPTION_PADDING = 10
TEXT_THICKNESS = 2
LINE_HEIGHT = 30


# ───────────────────────────────
# LINK PARSER
# ───────────────────────────────
def parse_telegram_link(link: str) -> Optional[Tuple[str, int]]:
    """
    Parse Telegram link and extract channel identifier and message ID.
    
    Supported formats:
    - https://t.me/channel_name/message_id
    - https://t.me/+channel_code/message_id
    - https://t.me/c/channel_id/message_id (private channel)
    - t.me/channel_name/message_id
    - @channel_name/message_id
    
    Args:
        link: Telegram link string
    
    Returns:
        Tuple of (channel_identifier, message_id) or None if invalid
    """
    link = link.strip()
    
    if not link:
        return None
    
    # Pattern 1: https://t.me/c/channel_id/message_id (private channel) - CHECK FIRST
    pattern_private = r'https?://t\.me/c/(\d+)/(\d+)'
    match = re.search(pattern_private, link)
    if match:
        channel_id = int(match.group(1))
        msg_id = int(match.group(2))
        return (f"-100{channel_id}", msg_id)  # Convert to supergroup ID format
    
    # Pattern 2: https://t.me/channel/message_id or https://t.me/+code/message_id
    pattern1 = r'https?://t\.me/([^/?\s]+)/(\d+)'
    match = re.search(pattern1, link)
    if match:
        channel = match.group(1)
        msg_id = int(match.group(2))
        return (channel, msg_id)
    
    # Pattern 3: t.me/channel/message_id (no https)
    pattern3 = r't\.me/([^/?\s]+)/(\d+)'
    match = re.search(pattern3, link)
    if match:
        channel = match.group(1)
        msg_id = int(match.group(2))
        return (channel, msg_id)
    
    # Pattern 4: @channel_name/message_id
    pattern4 = r'@([^/?\s]+)/(\d+)'
    match = re.search(pattern4, link)
    if match:
        channel = match.group(1)
        msg_id = int(match.group(2))
        return (channel, msg_id)
    
    return None


def extract_links_from_text(text: str) -> list:
    """
    Extract all Telegram links from text.
    
    Args:
        text: Text containing telegram links
    
    Returns:
        List of (channel, message_id) tuples
    """
    links = []
    # Find all potential telegram links
    pattern = r'(?:https?://)?t\.me/[^\s]*|@[^\s/]+/\d+'
    matches = re.findall(pattern, text)
    
    for match in matches:
        parsed = parse_telegram_link(match)
        if parsed:
            links.append(parsed)
    
    return links


# ───────────────────────────────
# FONT MANAGEMENT
# ───────────────────────────────
def ensure_bangla_font() -> str:
    """
    Ensure Bangla font exists. Download if necessary.
    Returns the path to the font file.
    """
    FONTS_DIR.mkdir(exist_ok=True, parents=True)
    
    if BANGLA_FONT_PATH.exists():
        return str(BANGLA_FONT_PATH)
    
    print(f"[CAPTION] Downloading Bangla font...")
    try:
        urllib.request.urlretrieve(BANGLA_FONT_URL, str(BANGLA_FONT_PATH))
        print(f"[CAPTION] ✅ Font downloaded to {BANGLA_FONT_PATH}")
        return str(BANGLA_FONT_PATH)
    except Exception as e:
        print(f"[CAPTION] ⚠️ Could not download font: {e}")
        return None


def get_pil_font(size: int = CAPTION_FONT_SIZE) -> Optional[ImageFont.FreeTypeFont]:
    """Get PIL font object for Bangla text rendering."""
    if not Image or not ImageFont:
        return None
    
    font_path = ensure_bangla_font()
    if not font_path:
        return None
    
    try:
        return ImageFont.truetype(font_path, size)
    except Exception as e:
        print(f"[CAPTION] Error loading font: {e}")
        return None


# ───────────────────────────────
# CAPTION TEXT PROCESSING
# ───────────────────────────────
def format_caption_text(
    caption: str,
    tags: List[str],
    include_links: bool = True,
    extracted_links: Optional[List[Tuple[str, int]]] = None,
    include_timestamp: bool = True,
    max_width: int = 50
) -> str:
    """
    Format caption text with tags, links (if found), and optional timestamp.
    
    Args:
        caption: Main caption text
        tags: List of tags to include as hashtags
        include_links: Whether to include extracted links in caption
        extracted_links: List of (channel, message_id) tuples extracted from text
        include_timestamp: Add timestamp to caption
        max_width: Maximum characters per line
    
    Returns:
        Formatted caption text
    """
    lines = []
    
    # Add main caption
    if caption:
        words = caption.split()
        current_line = ""
        for word in words:
            if len(current_line) + len(word) + 1 <= max_width:
                current_line += word + " "
            else:
                if current_line:
                    lines.append(current_line.strip())
                current_line = word + " "
        if current_line:
            lines.append(current_line.strip())
    
    # Add extracted links if any
    if include_links and extracted_links:
        lines.append("")  # Empty line separator
        lines.append("📌 Sources:")
        for channel, msg_id in extracted_links:
            lines.append(f"  • t.me/{channel}/{msg_id}")
    
    # Add tags as hashtags
    if tags:
        lines.append("")  # Empty line separator
        tag_line = " ".join([f"#{tag}" for tag in tags])
        # Wrap hashtags if too long
        if len(tag_line) > max_width:
            words = tag_line.split()
            current_line = ""
            for word in words:
                if len(current_line) + len(word) + 1 <= max_width:
                    current_line += word + " "
                else:
                    if current_line:
                        lines.append(current_line.strip())
                    current_line = word + " "
            if current_line:
                lines.append(current_line.strip())
        else:
            lines.append(tag_line)
    
    # Add timestamp if enabled
    if include_timestamp:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        lines.append(f"📅 {timestamp}")
    
    return "\n".join(lines)


# ───────────────────────────────
# VIDEO CAPTION RENDERING
# ───────────────────────────────
def add_caption_to_frame(
    frame: np.ndarray,
    caption: str,
    position: str = "top",
    font_size: int = CAPTION_FONT_SIZE,
    font_color: Tuple = CAPTION_COLOR,
    bg_color: Tuple = CAPTION_BG_COLOR
) -> np.ndarray:
    """
    Add caption text to a video frame using OpenCV.
    
    Args:
        frame: Input frame (BGR format)
        caption: Caption text
        position: Vertical position ("top" or "bottom")
        font_size: Font size (note: OpenCV doesn't use TTF directly, uses scale)
        font_color: Text color in BGR
        bg_color: Background color in BGR
    
    Returns:
        Frame with caption added
    """
    if caption is None or caption == "":
        return frame
    
    frame_h, frame_w = frame.shape[:2]
    font = cv2.FONT_HERSHEY_SIMPLEX
    font_scale = font_size / 30.0  # Approximate scaling
    thickness = TEXT_THICKNESS
    
    # Split caption into lines
    lines = caption.split("\n")
    
    # Calculate text size
    line_height = LINE_HEIGHT
    text_width = 0
    text_height = len(lines) * line_height + CAPTION_PADDING * 2
    
    for line in lines:
        (text_w, text_h), _ = cv2.getTextSize(line, font, font_scale, thickness)
        text_width = max(text_width, text_w)
    
    text_width += CAPTION_PADDING * 2
    
    # Determine position
    if position.lower() == "top":
        y_start = 0
    else:
        y_start = frame_h - text_height
    
    # Draw background rectangle
    cv2.rectangle(
        frame,
        (0, y_start),
        (text_width + CAPTION_PADDING, y_start + text_height),
        bg_color,
        -1
    )
    
    # Draw text
    y_offset = y_start + CAPTION_PADDING + 20
    for line in lines:
        cv2.putText(
            frame,
            line,
            (CAPTION_PADDING, y_offset),
            font,
            font_scale,
            font_color,
            thickness,
            cv2.LINE_AA
        )
        y_offset += line_height
    
    return frame


def add_captions_to_video(
    input_video: str,
    output_video: str,
    caption: str,
    tags: List[str],
    position: str = "top",
    include_timestamp: bool = True,
    extracted_links: Optional[List[Tuple[str, int]]] = None,
    fps: Optional[int] = None
) -> bool:
    """
    Add captions to a video file.
    
    Args:
        input_video: Path to input video file
        output_video: Path to output video file  
        caption: Caption text
        tags: List of tags to add as hashtags
        position: Vertical position ("top" or "bottom")
        include_timestamp: Include timestamp in caption
        extracted_links: List of (channel, message_id) tuples to include
        fps: Optional FPS override (if None, uses source FPS)
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Format the caption with tags and links
        full_caption = format_caption_text(
            caption, tags, 
            include_links=True,
            extracted_links=extracted_links,
            include_timestamp=include_timestamp
        )
        
        # Open video
        cap = cv2.VideoCapture(input_video)
        if not cap.isOpened():
            print(f"[CAPTION] ❌ Could not open video: {input_video}")
            return False
        
        # Get video properties
        frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fps_val = fps or int(cap.get(cv2.CAP_PROP_FPS))
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        
        # Setup video writer
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        out = cv2.VideoWriter(output_video, fourcc, fps_val, (frame_width, frame_height))
        
        if not out.isOpened():
            print(f"[CAPTION] ❌ Could not create output video: {output_video}")
            cap.release()
            return False
        
        print(f"[CAPTION] Processing {total_frames} frames...")
        frame_count = 0
        
        # Process frames
        while True:
            ret, frame = cap.read()
            if not ret:
                break
            
            # Add caption to frame
            frame_with_caption = add_caption_to_frame(
                frame, full_caption, position
            )
            
            out.write(frame_with_caption)
            frame_count += 1
            
            # Progress indicator
            if frame_count % max(1, total_frames // 10) == 0:
                progress = (frame_count / total_frames) * 100
                print(f"[CAPTION] Progress: {progress:.1f}%")
        
        # Cleanup
        cap.release()
        out.release()
        
        print(f"[CAPTION] ✅ Video captioned successfully: {output_video}")
        return True
        
    except Exception as e:
        print(f"[CAPTION] ❌ Error adding captions: {e}")
        return False


def add_captions_to_files(
    file_paths: List[str],
    caption: str,
    tags: List[str],
    output_dir: Optional[str] = None,
    position: str = "top",
    include_timestamp: bool = True
) -> List[Tuple[str, bool]]:
    """
    Add captions to multiple video files.
    
    Args:
        file_paths: List of input video file paths
        caption: Caption text (same for all videos)
        tags: List of tags (same for all videos)
        output_dir: Optional output directory (if None, use same as input)
        position: Caption position
        include_timestamp: Include timestamp in caption
    
    Returns:
        List of tuples (file_path, success)
    """
    results = []
    
    # Extract links from caption once
    extracted_links = extract_links_from_text(caption)
    
    for file_path in file_paths:
        if not file_path.lower().endswith(('.mp4', '.mkv', '.mov', '.avi', '.webm')):
            results.append((file_path, False))
            continue
        
        # Determine output path
        if output_dir:
            output_path = Path(output_dir) / f"captioned_{Path(file_path).name}"
        else:
            base_path = Path(file_path)
            output_path = base_path.parent / f"captioned_{base_path.name}"
        
        # Add caption
        success = add_captions_to_video(
            file_path,
            str(output_path),
            caption,
            tags,
            position,
            include_timestamp,
            extracted_links=extracted_links
        )
        
        results.append((str(output_path), success))
    
    return results


# ───────────────────────────────
# UTILITY FUNCTIONS
# ───────────────────────────────
def process_tags(tags_input: str) -> List[str]:
    """
    Process comma-separated tags from user input.
    
    Args:
        tags_input: Comma-separated tag string
    
    Returns:
        List of tags
    """
    return [tag.strip() for tag in tags_input.split(",") if tag.strip()]


if __name__ == "__main__":
    # Test the integrated parser + caption generator
    print("[CAPTION] Testing integrated link parser + caption generator...")
    
    test_caption = "Test video with sources: https://t.me/testchannel/123 and tags"
    test_tags = ["টেলিগ্রাম", "ভিডিও", "সাবটাইটেল"]
    
    extracted = extract_links_from_text(test_caption)
    formatted = format_caption_text(test_caption, test_tags, extracted_links=extracted)
    print(f"Extracted links: {extracted}")
    print(f"Formatted caption:\n{formatted}")
