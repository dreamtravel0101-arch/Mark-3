@echo off
REM Telegram Task Bot - Optimized Speed Configuration
REM Sets environment variables for maximum download/upload speed

echo Starting bot with optimized speed settings...
echo.
echo Configuration:
echo   - Download Concurrency: 10 (default: 3)
echo   - Upload Concurrency: 5 (default: 1)
echo   - Upload Delays: Disabled
echo.

setlocal enabledelayedexpansion

REM Set optimized environment variables
set DOWNLOAD_CONCURRENCY=10
set UPLOAD_FILE_CONCURRENCY=5
set MIN_UPLOAD_DELAY=0
set MAX_UPLOAD_DELAY=0
set INTER_UPLOAD_DELAY=0
set UPLOAD_DELAY_FACTOR=0

REM Run the bot
python main.py

endlocal
pause
