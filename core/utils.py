# ============================================================
# SMART TELEGRAM ENTITY CORE v3.2-ABSOLUTE-HARDENED
# ✅ WindowsPath Immunity • Invite Safe • Cache Safe
# ✅ Zero Feature Loss • Atomic IO • Flood Safe
# ✅ Thread-safe JSON • Full drop-in replacement
# ============================================================

import os
import re
import json
import asyncio
import shutil
import threading
from pathlib import Path
from typing import List, Any
from telethon import TelegramClient, errors
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from telethon.tl.types import PeerChannel

# ─────────────────────────────────────────────
# TYPE ENFORCER
# ─────────────────────────────────────────────
def force_str(value) -> str:
    """Absolute immunity against WindowsPath leaks."""
    if isinstance(value, Path):
        return str(value)
    return str(value) if value is not None else ""

# ─────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent
SESSION_DIR = BASE_DIR / "sessions"
STORAGE_DIR = BASE_DIR / "storage"
DOWNLOAD_DIR = STORAGE_DIR / "downloads"
UPLOAD_DIR = STORAGE_DIR / "uploads"
CONFIG_DIR = BASE_DIR / "config"
ENTITIES_CACHE_FILE = CONFIG_DIR / "entities.json"
PROGRESS_FILE = CONFIG_DIR / "progress.json"
LEGACY_PROGRESS_FILE = BASE_DIR / "progress.json"

_json_lock = threading.RLock()

# ─────────────────────────────────────────────
# DIRECTORY SAFETY
# ─────────────────────────────────────────────
def ensure_dirs():
    for folder in [SESSION_DIR, DOWNLOAD_DIR, UPLOAD_DIR, CONFIG_DIR]:
        folder.mkdir(parents=True, exist_ok=True)

def migrate_progress_json():
    ensure_dirs()
    if LEGACY_PROGRESS_FILE.exists() and not PROGRESS_FILE.exists():
        try:
            shutil.move(str(LEGACY_PROGRESS_FILE), str(PROGRESS_FILE))
        except Exception:
            pass

migrate_progress_json()

# ─────────────────────────────────────────────
# SANITIZER
# ─────────────────────────────────────────────
def sanitize_path_name(name: str) -> str:
    name = force_str(name)
    sanitized = re.sub(r'[<>:"/\\|?*]', "_", name)
    sanitized = sanitized.strip().replace(" ", "_")
    return sanitized[:100] or "unnamed"

# ─────────────────────────────────────────────
# TELEGRAM CLIENT CREATION
# ─────────────────────────────────────────────
async def create_client(acc: dict) -> TelegramClient:
    ensure_dirs()
    import os, sqlite3

    # determine session file path
    phone = force_str(acc.get("phone", os.getenv("PHONE", "anon"))).replace("+", "")
    api_id = acc.get("api_id") or os.getenv("API_ID")
    api_hash = acc.get("api_hash") or os.getenv("API_HASH")

    # allow explicit session file from the account entry
    if acc.get("session"):
        custom = force_str(acc["session"])
        # strip any leading "sessions/" so users can copy the path from the
        # default storage location without duplicating
        if not os.path.isabs(custom):
            if custom.startswith("sessions/") or custom.startswith("sessions\\"):
                # drop the leading directory component
                custom = custom.split(os.sep, 1)[1] if os.sep in custom else custom
            session_file = str(SESSION_DIR / custom)
        else:
            session_file = custom
    else:
        session_file = str(SESSION_DIR / f"{phone}.session")

    # ensure the parent directory exists (Telethon does not create it)
    os.makedirs(os.path.dirname(session_file), exist_ok=True)
    print(f"Using session file: {session_file}")

    def cleanup_journal():
        # remove any SQLite auxiliary files that often cause locks
        for suffix in ("-journal", "-wal", "-shm"):
            fn = session_file + suffix
            if os.path.exists(fn):
                try:
                    os.remove(fn)
                    print(f"Removed locked journal file: {fn}")
                except Exception:
                    pass

    # try to connect with simple retry on database lock
    # ⚡ SPEED OPTIMIZATION: Disable flood sleep throttling for 3-5MB/s downloads
    client = TelegramClient(
        session_file, 
        api_id, 
        api_hash,
        flood_sleep_threshold=0,  # 🚀 Disable automatic throttling
        request_retries=5,         # More resilient on errors
        connection_retries=3,      # Better connection handling
    )
    try:
        await client.connect()
    except Exception as e:
        # catch common sqlite errors and provide helpful hints
        msg = str(e).lower()
        if "database is locked" in msg or isinstance(e, sqlite3.OperationalError):
            live_status(f"Session database locked for {session_file}, attempting recovery", "WARN")
            cleanup_journal()
            try:
                await client.connect()
            except Exception as e2:
                live_status(f"Failed to open session after cleanup: {e2}", "ERROR")
                raise
        elif "unable to open database file" in msg:
            live_status(
                f"Cannot open session file '{session_file}'. "
                f"Check that the path is correct and you have permission to write to it.",
                "ERROR"
            )
            raise
        else:
            raise

    if not await client.is_user_authorized():
        try:
            await client.send_code_request(acc.get("phone"))
            code = input(f"Enter code for {acc.get('phone')}: ").strip()
            await client.sign_in(acc.get("phone"), code)
        except errors.SessionPasswordNeededError:
            pw = input("Two-step password: ").strip()
            await client.sign_in(password=pw)
        except Exception as e:
            print(f"❌ Login failed: {e}")
            raise
    async for _ in client.iter_dialogs():
        break
    return client

# ─────────────────────────────────────────────
# IDENTIFIER NORMALIZATION
# ─────────────────────────────────────────────
def normalize_identifier(identifier: str) -> str:
    identifier = force_str(identifier).strip()
    identifier = identifier.replace("https://", "").replace("http://", "")
    identifier = identifier.replace("www.", "")
    if identifier.startswith("t.me/"):
        identifier = identifier.replace("t.me/", "")
    if identifier.startswith("joinchat/"):
        identifier = "+" + identifier.replace("joinchat/", "", 1)
    
    # Handle private channel URLs: c/CHANNEL_ID/MESSAGE_ID → -100CHANNEL_ID
    if identifier.startswith("c/"):
        parts = identifier.split("/")
        if len(parts) >= 2 and parts[1].isdigit():
            channel_id = parts[1]
            identifier = f"-100{channel_id}"
    
    return identifier

# ─────────────────────────────────────────────
# THREAD-SAFE JSON IO
# ─────────────────────────────────────────────

def _safe_json_loads(s: str):
    """Load JSON from a string, auto-fixing common Windows backslash mistakes.

    Many users paste Windows paths directly into JSON and forget to escape
    backslashes. The builtin parser then raises "Bad Unicode escape" errors
    for sequences like \\U. This helper attempts a quick repair by doubling
    any lone backslashes when such an error occurs.
    """
    try:
        return json.loads(s)
    except json.JSONDecodeError as e:
        msg = str(e)
        # only attempt auto-fix if error is due to a bad unicode escape
        if "unicode escape" in msg.lower():
            fixed = s.replace("\\", "\\\\")
            try:
                return json.loads(fixed)
            except json.JSONDecodeError:
                pass
        # re-raise original error for callers to handle
        raise


def read_json(path: Path, default=None):
    try:
        if not path.exists():
            return default
        text = path.read_text(encoding="utf-8")
        return _safe_json_loads(text)
    except Exception:
        return default


# ───────────────────────────────
# CAPTION / TEXT EXTRACTION
# ───────────────────────────────

def build_caption(messages: List[Any]) -> str:
    """
    Extract and preserve full message content, including:
    - All text content with formatting
    - Embedded URLs and links
    - Mentions, hashtags, and other formatted entities
    - Media captions
    """
    parts = []
    seen = set()

    for msg in messages:
        # Try to get the message text - this includes parsed entities
        text = None

        # First, try the .text property (includes formatted text)
        if hasattr(msg, "text"):
            text = getattr(msg, "text", None)

        # If no text, try message property
        if not text and hasattr(msg, "message"):
            text = getattr(msg, "message", None)

        # Add the text if it exists and hasn't been seen
        if text and text.strip() and text not in seen:
            parts.append(text.strip())
            seen.add(text)

        # Also extract any raw URLs from entities (caption links, button URLs, etc.)
        if hasattr(msg, "entities") and msg.entities:
            for ent in msg.entities:
                # Get URL from text_url entity
                url = getattr(ent, "url", None)
                if url and url not in seen:
                    parts.append(url)
                    seen.add(url)

    return "\n".join(parts).strip()


def write_json(path: Path, data):
    with _json_lock:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            with tmp.open("w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            tmp.replace(path)
        except Exception:
            pass

# ─────────────────────────────────────────────
# ENTITY CACHE
# ─────────────────────────────────────────────
def load_entities_cache():
    return read_json(ENTITIES_CACHE_FILE, {})

def save_entities_cache(data):
    write_json(ENTITIES_CACHE_FILE, data)

# ─────────────────────────────────────────────
# DIALOG RESOLUTION
# ─────────────────────────────────────────────
async def resolve_from_dialogs(client: TelegramClient, channel_id: int):
    async for dialog in client.iter_dialogs():
        ent = dialog.entity
        if getattr(ent, "id", None) == channel_id:
            return ent
    return None

# ─────────────────────────────────────────────
# ULTRA SAFE ENTITY RESOLVER
# ─────────────────────────────────────────────
async def resolve_entity_safe(client: TelegramClient, identifier: str):
    identifier = normalize_identifier(identifier)
    cache = load_entities_cache()

    # 1️⃣ CACHE
    if identifier in cache:
        try:
            return await client.get_entity(PeerChannel(cache[identifier]["id"]))
        except Exception:
            cache.pop(identifier, None)
            save_entities_cache(cache)

    # 2️⃣ PRIVATE INVITE
    if identifier.startswith("+"):
        invite_hash = identifier[1:]
        try:
            updates = await client(ImportChatInviteRequest(invite_hash))
            entity = updates.chats[0]
            cache[identifier] = {"id": entity.id}
            save_entities_cache(cache)
            return entity
        except errors.UserAlreadyParticipantError:
            try:
                invite = await client(CheckChatInviteRequest(invite_hash))
                entity = invite.chat
                cache[identifier] = {"id": entity.id}
                save_entities_cache(cache)
                return entity
            except Exception:
                return None
        except errors.FloodWaitError as e:
            await asyncio.sleep(e.seconds)
            return None
        except Exception:
            return None

    # 3️⃣ NUMERIC ID
    if identifier.startswith("-100") and identifier[1:].isdigit():
        cid = int(identifier.replace("-100", ""))
        ent = await resolve_from_dialogs(client, cid)
        if ent:
            cache[identifier] = {"id": ent.id}
            save_entities_cache(cache)
            return ent
        try:
            full = await client(GetFullChannelRequest(PeerChannel(cid)))
            ent = full.chats[0]
            cache[identifier] = {"id": ent.id}
            save_entities_cache(cache)
            return ent
        except Exception:
            return None

    # 4️⃣ USERNAME
    try:
        ent = await client.get_entity(identifier)
        cache[identifier] = {"id": ent.id}
        save_entities_cache(cache)
        return ent
    except errors.FloodWaitError as e:
        await asyncio.sleep(e.seconds)
        return None
    except Exception:
        return None

# ─────────────────────────────────────────────
# SAFE JOIN
# ─────────────────────────────────────────────
async def ensure_join(client: TelegramClient, target: str):
    target = force_str(target)
    entity = await resolve_entity_safe(client, target)
    if not entity:
        print(f"❌ Could not resolve target {target}")
        return None
    try:
        await client(JoinChannelRequest(entity))
    except errors.UserAlreadyParticipantError:
        pass
    except errors.FloodWaitError as e:
        print(f"⏳ Join FloodWait {e.seconds}s")
        await asyncio.sleep(e.seconds)
    except Exception as e:
        print(f"❌ Could not join: {type(e).__name__}: {e}")
    return entity

# ─────────────────────────────────────────────
# PROGRESS WRAPPERS
# ─────────────────────────────────────────────
def load_progress(default=None):
    migrate_progress_json()
    return read_json(PROGRESS_FILE, default or {})

def save_progress(data):
    write_json(PROGRESS_FILE, data)

# ─────────────────────────────────────────────
# VERBOSE FLAG
# ─────────────────────────────────────────────
VERBOSE = True

print("🎯 entity_core.py v3.2 HARDENED loaded.")