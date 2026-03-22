import os
import shutil
from pathlib import Path
from core.utils import load_progress

# Adjust these paths as needed
RELAY_DIR = Path('storage/relay')
MIRROR_DIR = Path('storage/mirror')

# Load processed posts from progress.json
progress = load_progress({})
processed_ids = set()
for k in progress:
    # k format: chat_id:post_id or similar
    try:
        post_id = str(k).split(':')[-1]
        processed_ids.add(post_id)
    except Exception:
        continue

def clean_dir(base_dir):
    if not base_dir.exists():
        return
    for sub in base_dir.iterdir():
        if sub.is_dir():
            # Folder name is post_id or similar
            if sub.name in processed_ids:
                print(f"Deleting {sub}")
                shutil.rmtree(sub, ignore_errors=True)

if __name__ == "__main__":
    print("Cleaning relay...")
    clean_dir(RELAY_DIR)
    print("Cleaning mirror...")
    clean_dir(MIRROR_DIR)
    print("Cleanup complete.")
