@echo off
REM Telegram Task Bot - Balanced Speed Configuration
REM Sets environment variables for good speed with stability

echo Starting bot with balanced speed settings...
echo.
echo Configuration:
echo   - Download Concurrency: 5 (default: 3)
echo   - Upload Concurrency: 3 (default: 1)
echo   - Min Upload Delay: 0.2 seconds
echo   - Max Upload Delay: 0.5 seconds
echo.

setlocal enabledelayedexpansion

REM Set balanced environment variables
set DOWNLOAD_CONCURRENCY=5
set UPLOAD_FILE_CONCURRENCY=3
set MIN_UPLOAD_DELAY=0.2
set MAX_UPLOAD_DELAY=0.5
set INTER_UPLOAD_DELAY=0.2
set UPLOAD_DELAY_FACTOR=1

REM Run the bot
python main.py

endlocal
pause
