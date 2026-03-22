import json
from pathlib import Path
from core.utils import BASE_DIR

CHANNELS_FILE = BASE_DIR / "config/channels.json"

def load_channels():
    """Load saved channels from JSON file."""
    if not CHANNELS_FILE.exists():
        return []
    try:
        from core.utils import read_json
        data = read_json(CHANNELS_FILE, {})
        return data.get("channels", []) if isinstance(data, dict) else []
    except Exception:
        return []

def save_channel(name, identifier):
    """Add a new channel to channels.json"""
    channels = load_channels()
    # Prevent duplicates
    for ch in channels:
        if ch["id"] == identifier or ch["name"].lower() == name.lower():
            return
    channels.append({"name": name, "id": identifier})
    CHANNELS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CHANNELS_FILE, "w", encoding="utf-8") as f:
        json.dump({"channels": channels}, f, indent=4)
