import json
import re
from pathlib import Path
from core.utils import BASE_DIR

PROGRESS_FILE = BASE_DIR / "config/progress.json"
BACKUP_FILE = BASE_DIR / "config/progress_backup_before_migration.json"


def extract_channel_id(key: str):
    """
    Extract numeric channel ID from:
    "Channel(id=3209175125, title='...', ...)"
    """
    match = re.search(r"id=(\d+)", key)
    if match:
        return match.group(1)
    return key  # already clean


def migrate():
    if not PROGRESS_FILE.exists():
        print("No progress.json found.")
        return

    with PROGRESS_FILE.open("r", encoding="utf-8") as f:
        data = json.load(f)

    # Backup first
    with BACKUP_FILE.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    print("✅ Backup created:", BACKUP_FILE)

    uploads = data.get("uploads", {})
    new_uploads = {}

    for raw_key, value in uploads.items():
        clean_id = extract_channel_id(str(raw_key))

        if clean_id not in new_uploads:
            new_uploads[clean_id] = {}

        # Merge safely (no overwriting existing msg entries)
        if isinstance(value, dict):
            for k, v in value.items():
                new_uploads[clean_id][k] = v

    data["uploads"] = new_uploads

    # Clean last_processed keys too
    last_processed = data.get("last_processed", {})
    new_last_processed = {}

    for key, val in last_processed.items():
        if "__TO__" in key:
            src, tgt = key.split("__TO__", 1)
            src_clean = extract_channel_id(src)
            tgt_clean = extract_channel_id(tgt)
            new_key = f"{src_clean}__TO__{tgt_clean}"
        else:
            new_key = extract_channel_id(key)

        new_last_processed[new_key] = val

    data["last_processed"] = new_last_processed

    with PROGRESS_FILE.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    print("🎉 Migration complete.")
    print("Old channel object keys → Converted to numeric IDs.")


if __name__ == "__main__":
    migrate()