# ============================================================
# TAG FILTER MODULE v2.0 ADVANCED
# Filters messages by content type, size, caption, patterns
# Supports: whitelist/blacklist, regex, size limits, channel rules
# ============================================================

import json
import os
import re
from pathlib import Path
from typing import List, Optional, Set, Dict, Any
from telethon.tl.types import Message

from core.utils import CONFIG_DIR, force_str

TAG_FILTERS_FILE = CONFIG_DIR / "tag_filters.json"

# Media type to tag mapping
MEDIA_TYPE_TAGS = {
    'video': ['video', 'mov', 'mp4', 'mkv', 'avi', 'webm', 'flv', 'ts', 'm3u8'],
    'photo': ['photo', 'jpg', 'jpeg', 'png', 'webp', 'gif', 'bmp', 'tiff'],
    'document': ['document', 'pdf', 'doc', 'docx', 'txt', 'zip', 'rar', '7z', 'tar', 'gz'],
    'audio': ['audio', 'mp3', 'aac', 'wav', 'ogg', 'flac', 'm4a'],
    'sticker': ['sticker'],
    'animation': ['animation', 'gif'],
}

# Reverse mapping: extension/mimetype to tags
EXTENSION_TO_TAGS = {}
for tag, extensions in MEDIA_TYPE_TAGS.items():
    for ext in extensions:
        if ext not in EXTENSION_TO_TAGS:
            EXTENSION_TO_TAGS[ext] = []
        EXTENSION_TO_TAGS[ext].append(tag)


class TagFilter:
    """Advanced tag-based filtering with multiple criteria."""
    
    def __init__(self):
        self.enabled = False
        self.include_tags: Set[str] = set()
        self.exclude_tags: Set[str] = set()
        self.include_keywords: List[str] = []  # Keywords to mark as priority
        self.exclude_keywords: List[str] = []  # Keywords to always skip
        
        # Advanced filters
        self.min_file_size = 0  # bytes
        self.max_file_size = 0  # 0 = unlimited
        self.filename_patterns: List[str] = []  # regex patterns (whitelist)
        self.filename_exclude: List[str] = []  # regex patterns (blacklist)
        self.caption_keywords: List[str] = []  # Legacy: keywords to search (deprecated, use include_keywords)
        self.caption_exclude: List[str] = []  # Legacy: keywords to avoid (deprecated, use exclude_keywords)
        self.channel_rules: Dict[int, Dict[str, Any]] = {}  # channel_id -> rules
        self.video_min_duration = 0  # seconds
        self.video_max_duration = 0  # 0 = unlimited
        self.min_resolution = 0  # pixels (e.g., 720 for HD)
        
        self._compiled_patterns = {}
        self.load_config()
    
    def load_config(self):
        """Load advanced tag filter configuration from JSON file."""
        if not TAG_FILTERS_FILE.exists():
            self._create_default_config()
            self.enabled = False
            return
        
        try:
            with open(TAG_FILTERS_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            self.enabled = config.get('enabled', False)
            self.include_tags = set(tag.lower().strip() for tag in config.get('include_tags', []))
            self.exclude_tags = set(tag.lower().strip() for tag in config.get('exclude_tags', []))
            
            # Keywords (new approach: separate include/exclude keywords)
            self.include_keywords = [k.lower().strip() for k in config.get('include_keywords', [])]
            self.exclude_keywords = [k.lower().strip() for k in config.get('exclude_keywords', [])]
            
            # Legacy: support old caption_keywords and caption_exclude fields
            # If caption_keywords is set but include_keywords is empty, use caption_keywords
            legacy_caption_keywords = config.get('caption_keywords', [])
            if legacy_caption_keywords and not self.include_keywords:
                self.include_keywords = [k.lower().strip() for k in legacy_caption_keywords]
            
            legacy_caption_exclude = config.get('caption_exclude', [])
            if legacy_caption_exclude and not self.exclude_keywords:
                self.exclude_keywords = [k.lower().strip() for k in legacy_caption_exclude]
            
            self.caption_keywords = self.include_keywords  # For backward compatibility
            self.caption_exclude = self.exclude_keywords  # For backward compatibility
            
            # Advanced filters
            size_limits = config.get('size_limits', {})
            self.min_file_size = self._parse_size(size_limits.get('min', '0'))
            self.max_file_size = self._parse_size(size_limits.get('max', '0'))
            
            self.filename_patterns = config.get('filename_patterns', [])
            self.filename_exclude = config.get('filename_exclude', [])
            self._compile_patterns()
            
            video_config = config.get('video_filters', {})
            self.video_min_duration = video_config.get('min_duration', 0)
            self.video_max_duration = video_config.get('max_duration', 0)
            self.min_resolution = video_config.get('min_resolution', 0)
            
            self.channel_rules = config.get('channel_rules', {})
            # Convert string keys to int
            self.channel_rules = {int(k): v for k, v in self.channel_rules.items()}
            
            if self.enabled:
                print(f"✅ Priority-based tag filters loaded")
                if self.include_tags:
                    print(f"   Include tags: {', '.join(sorted(self.include_tags))}")
                if self.include_keywords:
                    print(f"   Include keywords: {', '.join(sorted(self.include_keywords))}")
                if self.exclude_tags:
                    print(f"   Exclude tags: {', '.join(sorted(self.exclude_tags))}")
                if self.exclude_keywords:
                    print(f"   Exclude keywords: {', '.join(sorted(self.exclude_keywords))}")
                if self.min_file_size or self.max_file_size:
                    print(f"   File size: {self._format_size(self.min_file_size)} to {self._format_size(self.max_file_size) if self.max_file_size else '∞'}")
                if self.filename_patterns:
                    print(f"   Filename patterns: {len(self.filename_patterns)} rules")
                if self.caption_keywords:
                    print(f"   Caption keywords: {len(self.caption_keywords)} keywords")
                if self.channel_rules:
                    print(f"   Channel-specific rules: {len(self.channel_rules)} channels")
        except Exception as e:
            print(f"⚠ Failed to load tag filters: {e}")
            self.enabled = False
    
    def _compile_patterns(self):
        """Compile regex patterns for performance."""
        for pattern in self.filename_patterns + self.filename_exclude:
            try:
                if pattern not in self._compiled_patterns:
                    self._compiled_patterns[pattern] = re.compile(pattern, re.IGNORECASE)
            except Exception as e:
                print(f"⚠ Invalid regex pattern '{pattern}': {e}")
    
    def _parse_size(self, size_str: str) -> int:
        """Parse human-readable size to bytes (e.g., '10MB' -> 10485760)."""
        if isinstance(size_str, int):
            return size_str
        
        size_str = str(size_str).strip().upper()
        if not size_str or size_str == '0':
            return 0
        
        multipliers = {'B': 1, 'KB': 1024, 'MB': 1024**2, 'GB': 1024**3}
        for suffix, mult in multipliers.items():
            if size_str.endswith(suffix):
                try:
                    return int(size_str[:-len(suffix)].strip()) * mult
                except ValueError:
                    return 0
        try:
            return int(size_str)
        except ValueError:
            return 0
    
    def _format_size(self, bytes_val: int) -> str:
        """Format bytes to human-readable size."""
        if bytes_val == 0:
            return '0B'
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes_val < 1024:
                return f"{bytes_val:.1f}{unit}"
            bytes_val /= 1024
        return f"{bytes_val:.1f}TB"
    
    def _create_default_config(self):
        """Create a comprehensive default config file."""
        default_config = {
            "enabled": False,
            "filter_mode": "strict",
            "include_tags": ["video", "photo", "document"],
            "exclude_tags": ["audio", "sticker"],
            
            "size_limits": {
                "min": "0B",
                "max": "2GB"
            },
            
            "filename_patterns": [
                ".*\\.(mp4|mkv|webm)$"
            ],
            "filename_exclude": [
                ".*\\.tmp$",
                ".*sample.*"
            ],
            
            "caption_keywords": [],
            "caption_exclude": [],
            
            "video_filters": {
                "min_duration": 0,
                "max_duration": 0,
                "min_resolution": 0
            },
            
            "channel_rules": {}
        }
        try:
            TAG_FILTERS_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(TAG_FILTERS_FILE, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=2)
            print(f"ℹ Created advanced tag_filters.json at {TAG_FILTERS_FILE}")
        except Exception as e:
            print(f"⚠ Could not create default tag_filters.json: {e}")
    
    def get_message_tags(self, msg: Message) -> Set[str]:
        """Extract content type tags from a Telegram message."""
        tags = set()
        
        if not msg or not msg.media:
            return tags
        
        media = msg.media
        
        # Detect media type
        if hasattr(media, 'photo'):
            tags.add('photo')
        elif hasattr(media, 'document'):
            mime = getattr(media.document, 'mime_type', '').lower()
            filename = getattr(media.document, 'file_name', '').lower()
            
            # Detect based on MIME type
            if 'video' in mime:
                tags.add('video')
            elif 'audio' in mime:
                tags.add('audio')
            elif 'application/pdf' in mime or filename.endswith('.pdf'):
                tags.add('document')
            elif 'application' in mime or 'text' in mime:
                tags.add('document')
            
            # Additional tag from filename extension
            if filename:
                ext = filename.split('.')[-1].lower()
                if ext in EXTENSION_TO_TAGS:
                    tags.update(EXTENSION_TO_TAGS[ext])
        
        elif hasattr(media, 'video'):
            tags.add('video')
        elif hasattr(media, 'audio'):
            tags.add('audio')
        elif hasattr(media, 'voice'):
            tags.add('audio')
        elif hasattr(media, 'animation'):
            tags.add('animation')
        elif hasattr(media, 'sticker'):
            tags.add('sticker')
        
        return tags if tags else {'unknown'}
    
    def _check_file_size(self, msg: Message) -> bool:
        """Check if file size is within limits."""
        if not msg.media:
            return True
        
        media = msg.media
        file_size = getattr(media, 'size', 0) or getattr(media, 'file_size', 0)
        
        if not file_size:
            return True  # No size info, allow
        
        if self.min_file_size and file_size < self.min_file_size:
            print(f"ℹ Message {msg.id} below min size ({self._format_size(file_size)})")
            return False
        
        if self.max_file_size and file_size > self.max_file_size:
            print(f"ℹ Message {msg.id} exceeds max size ({self._format_size(file_size)})")
            return False
        
        return True
    
    def _check_filename(self, msg: Message) -> bool:
        """Check if filename matches patterns."""
        if not msg.media or not hasattr(msg.media, 'document'):
            return True
        
        filename = getattr(msg.media.document, 'file_name', '')
        if not filename:
            return True
        
        # Check exclude patterns first
        for pattern in self.filename_exclude:
            compiled = self._compiled_patterns.get(pattern)
            if compiled and compiled.match(filename):
                print(f"ℹ Message {msg.id} excluded by filename pattern: {filename}")
                return False
        
        # Check include patterns
        if self.filename_patterns:
            match_found = False
            for pattern in self.filename_patterns:
                compiled = self._compiled_patterns.get(pattern)
                if compiled and compiled.match(filename):
                    match_found = True
                    break
            if not match_found:
                print(f"ℹ Message {msg.id} doesn't match filename patterns: {filename}")
                return False
        
        return True
    
    def _check_caption(self, msg: Message) -> bool:
        """Check caption for keywords."""
        text = (msg.message or '').lower()
        
        # Check exclude keywords first
        for keyword in self.caption_exclude:
            if keyword in text:
                print(f"ℹ Message {msg.id} excluded by caption keyword: {keyword}")
                return False
        
        # Check include keywords
        if self.caption_keywords:
            match_found = any(k in text for k in self.caption_keywords)
            if not match_found:
                print(f"ℹ Message {msg.id} doesn't contain required keywords")
                return False
        
        return True
    
    def should_process_message(self, msg: Message) -> bool:
        """
        Priority-based filtering system:
        
        Priority 1 (ALWAYS): Check EXCLUDE - if tag/text matches, SKIP completely
        Priority 2 (HIGHLIGHT): Check INCLUDE - if tag/text matches, PROCESS (marked priority)
        Priority 3 (DEFAULT): Not in either - PROCESS anyway
        
        Empty lists = don't filter by that criteria
        
        Returns:
            True if message should be processed, False if it should be skipped.
        """
        if not self.enabled:
            return True
        
        # Check channel-specific rules
        channel_id = getattr(msg, 'chat_id', None)
        if channel_id in self.channel_rules:
            rule = self.channel_rules[channel_id]
            if not rule.get('enabled', True):
                return False
        
        msg_tags = self.get_message_tags(msg)
        msg_caption = (msg.message or '').lower()
        
        # ═══════════════════════════════════════════════════════════
        # PRIORITY 1: EXCLUDE - Hard blacklist (checked first)
        # ═══════════════════════════════════════════════════════════
        
        # Check exclude tags
        if self.exclude_tags and msg_tags & self.exclude_tags:
            reason = list(msg_tags & self.exclude_tags)[0]
            print(f"❌ Message {msg.id} EXCLUDED by tag '{reason}'")
            return False
        
        # Check exclude keywords in caption
        if self.caption_exclude:
            for keyword in self.caption_exclude:
                if keyword in msg_caption:
                    print(f"❌ Message {msg.id} EXCLUDED by keyword '{keyword}'")
                    return False
        
        # ═══════════════════════════════════════════════════════════
        # PRIORITY 2: INCLUDE - Soft priority (process if matched)
        # ═══════════════════════════════════════════════════════════
        
        # Check include tags
        if self.include_tags and msg_tags & self.include_tags:
            reason = list(msg_tags & self.include_tags)[0]
            print(f"✅ Message {msg.id} INCLUDED by tag '{reason}' (priority)")
            # Continue to other checks (size, filename, etc.)
        
        # Check include keywords in caption
        elif self.caption_keywords:
            for keyword in self.caption_keywords:
                if keyword in msg_caption:
                    print(f"✅ Message {msg.id} INCLUDED by keyword '{keyword}' (priority)")
                    break
        
        else:
            # No include criteria matched, but still process (not in exclude)
            print(f"✓ Message {msg.id} processed (not in exclude/include lists)")
        
        # ═══════════════════════════════════════════════════════════
        # OTHER CHECKS: Size, filename, etc.
        # ═══════════════════════════════════════════════════════════
        
        # Check file size
        if not self._check_file_size(msg):
            return False
        
        # Check filename
        if not self._check_filename(msg):
            return False
        
        return True
    
    def filter_messages(self, messages: List[Message]) -> List[Message]:
        """Filter a list of messages based on all criteria."""
        if not self.enabled:
            return messages
        
        filtered = []
        for msg in messages:
            if self.should_process_message(msg):
                filtered.append(msg)
        
        return filtered


# Global tag filter instance
_tag_filter = None

def get_tag_filter() -> TagFilter:
    """Get or create the global tag filter instance."""
    global _tag_filter
    if _tag_filter is None:
        _tag_filter = TagFilter()
    return _tag_filter

def reload_tag_filters():
    """Reload tag filters from config (useful if config changes)."""
    global _tag_filter
    _tag_filter = TagFilter()

def should_process_message(msg: Message) -> bool:
    """Shortcut to check if a message should be processed."""
    return get_tag_filter().should_process_message(msg)

def filter_messages(messages: List[Message]) -> List[Message]:
    """Shortcut to filter a list of messages."""
    return get_tag_filter().filter_messages(messages)

