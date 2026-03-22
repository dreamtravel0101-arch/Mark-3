import os
import sys

print("=" * 60)
print("ENVIRONMENT VARIABLE CHECK")
print("=" * 60)

vars_to_check = [
    "DOWNLOAD_CONCURRENCY",
    "UPLOAD_FILE_CONCURRENCY", 
    "MIN_UPLOAD_DELAY",
    "MAX_UPLOAD_DELAY",
    "INTER_UPLOAD_DELAY",
    "UPLOAD_DELAY_FACTOR"
]

print("\nCurrent Environment Variables:")
for var in vars_to_check:
    value = os.getenv(var, "[NOT SET]")
    print(f"  {var}: {value}")

print("\n" + "=" * 60)
print("EXPECTED DEFAULTS (if not set):")
print("=" * 60)
print("  DOWNLOAD_CONCURRENCY: 8")
print("  UPLOAD_FILE_CONCURRENCY: 1")
print("  MIN_UPLOAD_DELAY: 0")
print("  MAX_UPLOAD_DELAY: 0")
print("  INTER_UPLOAD_DELAY: 0.5")
print("  UPLOAD_DELAY_FACTOR: 1.0")

print("\n" + "=" * 60)
print("Recommended settings for 2-account setup:")
print("=" * 60)
print("  DOWNLOAD_CONCURRENCY=8")
print("  UPLOAD_FILE_CONCURRENCY=2") 
print("  MIN_UPLOAD_DELAY=0")
print("  MAX_UPLOAD_DELAY=0")
print("  INTER_UPLOAD_DELAY=0.1")
