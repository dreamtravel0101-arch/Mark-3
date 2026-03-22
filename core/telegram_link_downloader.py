# ============================================================
# TELEGRAM LINK DOWNLOADER
# Download messages using parsed Telegram links
# ============================================================

import re
import asyncio
from typing import List, Optional, Tuple, Any
from pathlib import Path
import os
import sys

# Handle import for both main module and direct execution
try:
    from core.caption_with_links import parse_telegram_link, extract_links_from_text
except ImportError:
    from caption_with_links import parse_telegram_link, extract_links_from_text


# ───────────────────────────────
# LINK DOWNLOADER
# ───────────────────────────────
def parse_link_input(input_str: str) -> List[Tuple[str, int]]:
    """
    Parse user input (links, comma-separated links, or message IDs) and extract channel/message pairs.
    
    Supports:
    - Single link: https://t.me/channel/123
    - Multiple links: https://t.me/chan1/123, t.me/chan2/456
    - Channel@messageID format: @channel/123
    - Direct message IDs: 123, 456, 789
    
    Args:
        input_str: User input string
    
    Returns:
        List of (channel, message_id) tuples
    """
    results = []
    
    # Split by comma or newline
    entries = re.split(r'[,\n]', input_str)
    
    for entry in entries:
        entry = entry.strip()
        if not entry:
            continue
        
        # Try to parse as a full link
        parsed = parse_telegram_link(entry)
        if parsed:
            results.append(parsed)
        else:
            # Try to extract just message ID
            msg_id_match = re.search(r'(\d{1,10})', entry)
            if msg_id_match:
                msg_id = int(msg_id_match.group(1))
                if 0 < msg_id < 2147483647:  # 32-bit unsigned int range
                    results.append((None, msg_id))  # Channel will be determined by context
    
    return results


async def download_by_links(
    client: Any,
    link_input: str,
    default_channel: Optional[str] = None,
    output_dir: Optional[str] = None,
    on_progress=None
) -> List[Tuple[str, bool, int]]:
    """
    Download messages from parsed Telegram links.
    Supports both single media and albums (grouped media).
    
    Args:
        client: Telethon client
        link_input: Links or message IDs (comma/newline separated)
        default_channel: Default channel to use if link doesn't specify one
        output_dir: Output directory (uses default if None)
        on_progress: Progress callback function
    
    Returns:
        List of tuples (filename, success, message_id)
    """
    results = []
    parsed_links = parse_link_input(link_input)
    
    if not parsed_links:
        print("[LINK_DL] ❌ No valid links or message IDs found")
        return results
    
    total = len(parsed_links)
    print(f"[LINK_DL] Found {total} link(s) to process")
    
    for idx, (channel, msg_id) in enumerate(parsed_links, 1):
        try:
            # Use provided channel or default
            target_channel = channel or default_channel
            
            if not target_channel:
                print(f"[LINK_DL] ⚠️ [{idx}/{total}] No channel specified for message {msg_id}")
                results.append(("", False, msg_id))
                continue
            
            # Resolve channel
            try:
                entity = await client.get_entity(target_channel)
            except Exception as e:
                print(f"[LINK_DL] ⚠️ [{idx}/{total}] Could not resolve channel '{target_channel}': {e}")
                results.append(("", False, msg_id))
                continue
            
            # Get message
            try:
                message = await client.get_messages(entity, ids=[msg_id])
                if not message or len(message) == 0:
                    print(f"[LINK_DL] ⚠️ [{idx}/{total}] Message {msg_id} not found in {target_channel}")
                    results.append(("", False, msg_id))
                    continue
                
                msg = message[0] if isinstance(message, list) else message
                
                # Check if message has media
                if not msg.media:
                    print(f"[LINK_DL] ⚠️ [{idx}/{total}] Message {msg_id} has no media")
                    results.append(("", False, msg_id))
                    continue
                
                # Check if this is an album (grouped media)
                is_album = msg.grouped_id is not None
                
                # Extract caption from the message (applies to both single and album)
                caption_text = msg.text or msg.caption or ""
                
                if is_album:
                    # Fetch all messages in the album group
                    print(f"[LINK_DL] 📸 [{idx}/{total}] Album detected! Fetching all items...")
                    
                    # Get all messages with the same grouped_id
                    album_messages = []
                    
                    try:
                        # Fetch messages in a range to find all in the album group
                        # Use larger range for more reliable album detection
                        search_range = await client.get_messages(
                            entity, 
                            limit=None,
                            offset_id=msg_id,
                            reverse=False,
                            wait_time=0
                        )
                        
                        # Filter messages with same grouped_id
                        album_messages = []
                        async for m in search_range:
                            if m and m.grouped_id == msg.grouped_id:
                                album_messages.append(m)
                            if m and m.id < msg_id - 50:  # Stop if we go too far back
                                break
                    except:
                        # Fallback: try simpler range-based search
                        try:
                            search_range = await client.get_messages(
                                entity, 
                                ids=list(range(max(1, msg_id - 20), msg_id + 20))
                            )
                            album_messages = [
                                m for m in search_range 
                                if m and m.grouped_id == msg.grouped_id
                            ]
                        except:
                            album_messages = [msg]
                    
                    if not album_messages:
                        album_messages = [msg]
                    
                    print(f"[LINK_DL] Found {len(album_messages)} item(s) in album")
                    
                    # Download all items in album
                    for item_idx, album_item in enumerate(album_messages, 1):
                        try:
                            file_result = await _download_single_media(
                                client, album_item, entity, msg_id, 
                                output_dir, on_progress, 
                                f"[{idx}/{total}] Album {item_idx}/{len(album_messages)}",
                                caption_text=caption_text  # Pass original caption to save with each item
                            )
                            if file_result:
                                results.append(file_result)
                        except Exception as e:
                            print(f"[LINK_DL] ❌ Error downloading album item {item_idx}: {e}")
                            results.append(("", False, msg_id))
                else:
                    # Single media download - also pass the caption
                    file_result = await _download_single_media(
                        client, msg, entity, msg_id, 
                        output_dir, on_progress, f"[{idx}/{total}]",
                        caption_text=caption_text  # Pass caption for single media too
                    )
                    if file_result:
                        results.append(file_result)
                
            except Exception as e:
                print(f"[LINK_DL] ❌ [{idx}/{total}] Error downloading message {msg_id}: {e}")
                results.append(("", False, msg_id))
        
        except Exception as e:
            print(f"[LINK_DL] ❌ [{idx}/{total}] Unexpected error: {e}")
            results.append(("", False, 0))
    
    return results


async def _download_single_media(
    client: Any,
    msg: Any,
    entity: Any,
    msg_id: int,
    output_dir: Optional[str],
    on_progress,
    label: str,
    caption_text: Optional[str] = None
) -> Optional[Tuple[str, bool, int]]:
    """
    Download a single media item from a message.
    
    Args:
        client: Telethon client
        msg: Telegram message object
        entity: Telegram entity (channel/group)
        msg_id: Original message ID (for reference)
        output_dir: Output directory
        on_progress: Progress callback
        label: Label for logging (e.g., "[1/3] Album 1/3")
        caption_text: Optional caption text to save alongside the downloaded file
    
    Returns:
        Tuple (filename, success, message_id) or None
    """
    try:
        # Generate output filename
        base_filename = f"tg_{entity.id}_{msg.id}"
        
        # Determine file extension based on media type
        ext = ".unknown"
        filename_override = None
        
        if hasattr(msg.media, 'photo'):
            ext = ".jpg"
        elif hasattr(msg.media, 'document'):
            # Document can contain video, audio, image, or binary data
            # First, try to get original filename from document attributes
            has_filename = False
            
            if hasattr(msg.media.document, 'attributes'):
                for attr in msg.media.document.attributes:
                    # Check for filename attribute
                    if hasattr(attr, 'file_name'):
                        filename_override = attr.file_name
                        base_filename = Path(filename_override).stem
                        ext = Path(filename_override).suffix or ".bin"
                        has_filename = True
                        break
            
            # If no filename found, try to determine from MIME type
            if not has_filename and hasattr(msg.media.document, 'mime_type'):
                mime = msg.media.document.mime_type.lower()
                
                if 'video' in mime:
                    if 'mp4' in mime or 'x-msvideo' in mime:
                        ext = ".mp4"
                    elif 'quicktime' in mime or 'mov' in mime:
                        ext = ".mov"
                    elif 'mkv' in mime or 'x-matroska' in mime:
                        ext = ".mkv"
                    elif 'webm' in mime:
                        ext = ".webm"
                    elif 'avi' in mime:
                        ext = ".avi"
                    else:
                        ext = ".mp4"  # Default to mp4 for unknown video types
                elif 'audio' in mime:
                    if 'mp3' in mime:
                        ext = ".mp3"
                    elif 'aac' in mime:
                        ext = ".aac"
                    elif 'ogg' in mime or 'opus' in mime:
                        ext = ".ogg"
                    elif 'flac' in mime:
                        ext = ".flac"
                    elif 'wav' in mime:
                        ext = ".wav"
                    else:
                        ext = ".mp3"  # Default to mp3 for unknown audio
                elif 'image' in mime:
                    if 'jpeg' in mime or 'jpg' in mime:
                        ext = ".jpg"
                    elif 'png' in mime:
                        ext = ".png"
                    elif 'gif' in mime:
                        ext = ".gif"
                    elif 'webp' in mime:
                        ext = ".webp"
                    else:
                        ext = ".jpg"  # Default to jpg for unknown image types
                elif 'pdf' in mime:
                    ext = ".pdf"
                elif 'zip' in mime or 'compressed' in mime:
                    ext = ".zip"
                else:
                    ext = ".bin"  # Fallback for unknown types
            elif not has_filename:
                ext = ".bin"  # No filename and no MIME type info
        elif hasattr(msg.media, 'video'):
            ext = ".mp4"
        elif hasattr(msg.media, 'audio'):
            ext = ".mp3"
        elif hasattr(msg.media, 'voice'):
            ext = ".ogg"
        
        output_filename = base_filename + ext
        
        # Set output path
        if output_dir:
            output_path = Path(output_dir) / output_filename
        else:
            output_path = Path("downloads") / output_filename
        
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Download
        print(f"[LINK_DL] ⬇️ {label} Downloading {output_filename}...")
        await client.download_media(
            msg.media,
            file=str(output_path),
            progress_callback=on_progress
        )
        
        # Save caption if provided (for re-upload purposes)
        if caption_text:
            caption_file = output_path.parent / f"_caption_{output_path.stem}.txt"
            try:
                with open(str(caption_file), 'w', encoding='utf-8') as f:
                    f.write(caption_text)
            except:
                pass  # Ignore caption save errors
        
        print(f"[LINK_DL] ✅ {label} Downloaded: {output_filename}")
        return (str(output_path), True, msg_id)
        
    except Exception as e:
        print(f"[LINK_DL] ❌ Error in _download_single_media: {e}")
        return None



async def batch_download_links(
    client: Any,
    links_list: List[str],
    default_channel: Optional[str] = None,
    output_dir: Optional[str] = None,
    on_progress=None
) -> List[Tuple[str, bool, int]]:
    """
    Download from a list of links (one per entry).
    
    Args:
        client: Telethon client
        links_list: List of telegram links or message IDs
        default_channel: Default channel if not in link
        output_dir: Output directory
        on_progress: Progress callback
    
    Returns:
        List of results
    """
    combined_input = "\n".join(links_list)
    return await download_by_links(
        client,
        combined_input,
        default_channel=default_channel,
        output_dir=output_dir,
        on_progress=on_progress
    )


# ───────────────────────────────
# UTILITY FUNCTIONS
# ───────────────────────────────
def validate_links(input_str: str) -> Tuple[bool, List[str]]:
    """
    Validate that input contains valid links or message IDs.
    
    Args:
        input_str: User input
    
    Returns:
        (is_valid, list_of_errors_or_info)
    """
    parsed = parse_link_input(input_str)
    
    if not parsed:
        return False, ["No valid links or message IDs found"]
    
    info = []
    for channel, msg_id in parsed:
        if channel:
            info.append(f"✓ {channel}/{msg_id}")
        else:
            info.append(f"✓ Message ID: {msg_id} (channel will be specified separately)")
    
    return True, info


if __name__ == "__main__":
    # Test the link parser
    print("[LINK_DL] Testing link parser...")
    
    test_inputs = [
        "https://t.me/mychannel/123",
        "t.me/testchan/456",
        "@mychannel/789",
        "123, 456, 789",
        "https://t.me/chan1/111\nt.me/chan2/222\n@chan3/333",
    ]
    
    for test_input in test_inputs:
        print(f"\nInput: {test_input}")
        parsed = parse_link_input(test_input)
        print(f"Parsed: {parsed}")
