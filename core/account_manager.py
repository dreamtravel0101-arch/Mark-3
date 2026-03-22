# core/account_manager.py

import os
import json
import asyncio
from typing import List, Any
from telethon import TelegramClient, errors
from telethon.sessions import StringSession

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
CONFIG_PATH = os.path.join("config", "accounts.json")
DEFAULT_API_ID_KEY = "api_id"
DEFAULT_API_HASH_KEY = "api_hash"
DEFAULT_SESSION_KEY = "session"  # can be file path or string session


# ─────────────────────────────────────────────
# HELPER: Safe JSON Read/Write
# ─────────────────────────────────────────────
def _read_json(path: str, default=None):
    if not os.path.exists(path):
        return default
    try:
        # delegate to our core utils safe reader which handles bad escapes
        from core.utils import read_json
        from pathlib import Path
        return read_json(Path(path), default)
    except Exception:
        return default


def _write_json(path: str, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"⚠️ Failed to write {path}: {e}")


# ─────────────────────────────────────────────
# MODERN ASYNC CLIENT MANAGEMENT
# ─────────────────────────────────────────────
async def create_client_from_entry(entry: dict) -> TelegramClient:
    """
    Create and start a Telethon client from a JSON entry.
    Entry fields supported:
      - api_id (int)
      - api_hash (str)
      - session (str) -> can be path or StringSession
      - name (optional): friendly name
    """

    import os
    api_id = int(entry.get(DEFAULT_API_ID_KEY) or os.getenv("API_ID"))
    api_hash = entry.get(DEFAULT_API_HASH_KEY) or os.getenv("API_HASH")
    session = entry.get(DEFAULT_SESSION_KEY, None) or entry.get("name", None) or os.getenv("PHONE", "anon")

    # Detect session type
    # ⚡ SPEED OPTIMIZATION: Disable flood sleep throttling for 3-5MB/s downloads
    client_config = dict(
        flood_sleep_threshold=0,  # 🚀 Disable automatic throttling
        request_retries=5,         # More resilient on errors
        connection_retries=3,      # Better connection handling
    )
    
    if os.path.exists(session) and session.endswith((".session", ".session-journal")):
        client = TelegramClient(session, api_id, api_hash, **client_config)
    else:
        try:
            if isinstance(session, str) and len(session) > 50:
                # Likely a StringSession
                client = TelegramClient(StringSession(session), api_id, api_hash, **client_config)
            else:
                sess_name = os.path.join("config", f"{session}.session")
                client = TelegramClient(sess_name, api_id, api_hash, **client_config)
        except Exception:
            client = TelegramClient(session, api_id, api_hash, **client_config)

    await client.connect()

    if not await client.is_user_authorized():
        print(f"⚠️ Client for '{session}' is NOT authorized. Please login manually.")
    else:
        print(f"✅ Client '{entry.get('name', session)}' connected successfully.")

    return client


async def get_active_clients(config_path: str = CONFIG_PATH) -> List[Any]:
    """Load all accounts from JSON and return connected TelegramClient list."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"accounts.json not found at {config_path}")

    entries = _read_json(config_path, default=[])
    if not isinstance(entries, list):
        entries = []

    clients = []
    for entry in entries:
        if not isinstance(entry, dict):
            print(f"⚠️ Skipping invalid account entry: {entry}")
            continue
        try:
            client = await create_client_from_entry(entry)
            clients.append(client)
        except Exception as e:
            print(f"❌ Failed to create client for {entry.get('name', entry)}: {e}")
    return clients


async def disconnect_clients(clients: List[Any]):
    """Disconnect all active clients safely."""
    for c in clients:
        try:
            await c.disconnect()
        except Exception:
            pass


# ─────────────────────────────────────────────
# LEGACY BACKWARD COMPATIBILITY FUNCTIONS
# (for older main.py versions)
# ─────────────────────────────────────────────
def list_accounts() -> list:
    """Return list of accounts from config/accounts.json"""
    data = _read_json(CONFIG_PATH, default=[])
    if not isinstance(data, list):
        return []
    return data


def add_account(phone_or_name: str, api_id: Any, api_hash: str, session: str = ""):
    """
    Add or update an account entry to accounts.json (legacy compatible).
    Automatically detects if it's a phone-based or named account.
    Works with both dict-based and string-based legacy data.
    """
    os.makedirs("config", exist_ok=True)
    accounts = list_accounts()

    # Normalize old entries
    normalized = []
    for acc in accounts:
        if isinstance(acc, str):
            normalized.append({"phone": acc, "api_id": None, "api_hash": None, "session": f"config/{acc}.session"})
        elif isinstance(acc, dict):
            normalized.append(acc)
    accounts = normalized

    # Check if exists
    existing = None
    for acc in accounts:
        if acc.get("name") == phone_or_name or acc.get("phone") == phone_or_name:
            existing = acc
            break

    if existing:
        existing.update({"api_id": int(api_id), "api_hash": api_hash})
        print(f"🔄 Updated existing account: {phone_or_name}")
    else:
        new_entry = {
            "name": phone_or_name,
            "phone": phone_or_name if phone_or_name.startswith("+") else None,
            "api_id": int(api_id),
            "api_hash": api_hash,
            "session": session or f"config/{phone_or_name}.session",
        }
        accounts.append(new_entry)
        print(f"✅ Added new account: {phone_or_name}")

    _write_json(CONFIG_PATH, accounts)


# ─────────────────────────────────────────────
# SELF TEST
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print("Testing account manager...\n")

    async def test():
        try:
            clients = await get_active_clients()
            print(f"Loaded {len(clients)} client(s).")
            await disconnect_clients(clients)
        except Exception as e:
            print("Error:", e)

    asyncio.run(test())
