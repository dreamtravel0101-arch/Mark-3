"""Resolve entries in config/scanner_exclude.json to numeric Telegram IDs.

Usage: python tools/resolve_excludes.py

It will try to use the first account in config/accounts.json as the session.
If that fails, it will prompt for `api_id` and `api_hash`.

The script backs up the original JSON before writing resolved IDs.
"""
import json
import shutil
import time
from pathlib import Path

from telethon import TelegramClient

BASE = Path(__file__).resolve().parents[1]
EXCLUDE_FILE = BASE / "config" / "scanner_exclude.json"
ACCOUNTS_FILE = BASE / "config" / "accounts.json"


def load_excludes():
    if not EXCLUDE_FILE.exists():
        print(f"Exclude file not found: {EXCLUDE_FILE}")
        return []
    return json.loads(EXCLUDE_FILE.read_text(encoding="utf-8"))


def save_excludes(data):
    # backup
    bak = EXCLUDE_FILE.with_suffix(f".bak_{int(time.time())}")
    shutil.copy(EXCLUDE_FILE, bak)
    EXCLUDE_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def get_account():
    if not ACCOUNTS_FILE.exists():
        return None
    try:
        arr = json.loads(ACCOUNTS_FILE.read_text(encoding="utf-8"))
        if not arr:
            return None
        acc = arr[0]
        return acc
    except Exception:
        return None


async def main():
    excludes = load_excludes()
    if not excludes:
        print("No excludes found.")
        return

    acc = get_account()
    client = None


    import os
    if acc:
        api_id = acc.get("api_id") or os.getenv("API_ID")
        api_hash = acc.get("api_hash") or os.getenv("API_HASH")
        session = acc.get("session") or os.getenv("PHONE", "session")
        session_path = (BASE / session) if not str(session).startswith("/") else Path(session)
        print(f"Using account from {ACCOUNTS_FILE}, session: {session_path}")
        client = TelegramClient(str(session_path), api_id, api_hash)
    else:
        print("No account found in config/accounts.json.\nYou can enter API credentials to resolve entries or set API_ID/API_HASH as env vars.")
        api_id = int(os.getenv("API_ID") or input("API_ID: ").strip())
        api_hash = os.getenv("API_HASH") or input("API_HASH: ").strip()
        client = TelegramClient("resolve_excludes_session", api_id, api_hash)

    await client.start()

    resolved = []
    changed = False

    for item in excludes:
        s = str(item).strip()
        if not s:
            continue
        # Skip numeric entries
        if s.lstrip("-").isdigit():
            resolved.append(s)
            continue
        try:
            ent = await client.get_entity(s)
            eid = getattr(ent, "id", None) or getattr(ent, "channel_id", None)
            if eid is not None:
                print(f"Resolved {s} -> {eid}")
                resolved.append(str(int(eid)))
                changed = True
            else:
                print(f"Could not resolve numeric id for {s}, keeping original")
                resolved.append(s)
        except Exception as e:
            print(f"Failed to resolve {s}: {e}")
            resolved.append(s)

    await client.disconnect()

    if changed:
        save_excludes(resolved)
        print(f"Updated {EXCLUDE_FILE} with resolved IDs (backup created).")
    else:
        print("No changes made.")


if __name__ == '__main__':
    import asyncio

    asyncio.run(main())
