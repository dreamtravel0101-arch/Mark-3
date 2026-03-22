@echo off
REM Telegram Task Bot - Optimized for 2-Account Setup
REM Runs environment check first, then starts bot

echo.
echo ========================================
echo 2-ACCOUNT OPTIMIZED Speed Setup
echo ========================================
echo.

REM First check environment variables
python check_env.py

echo.
echo ========================================
echo Starting bot with optimized settings...
echo ========================================
echo.

setlocal enabledelayedexpansion

REM Set optimized environment variables for 2-account setup
set DOWNLOAD_CONCURRENCY=8
set UPLOAD_FILE_CONCURRENCY=2
set MIN_UPLOAD_DELAY=0
set MAX_UPLOAD_DELAY=0
set INTER_UPLOAD_DELAY=0.1
set UPLOAD_DELAY_FACTOR=1

echo Active Configuration:
echo   DOWNLOAD_CONCURRENCY: 8 (max concurrent downloads)
echo   UPLOAD_FILE_CONCURRENCY: 2 (2 files at a time for 2 accounts)
echo   INTER_UPLOAD_DELAY: 0.1 seconds (minimal spacing)
echo.

python main.py

endlocal
pause
