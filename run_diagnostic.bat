@echo off
REM Diagnostic script to test bot speed and configuration

echo ========================================
echo Telegram Task Bot - Speed Diagnostic
echo ========================================
echo.

echo Current Environment Variables:
echo   DOWNLOAD_CONCURRENCY: %DOWNLOAD_CONCURRENCY% ^(default: 8^)
echo   UPLOAD_FILE_CONCURRENCY: %UPLOAD_FILE_CONCURRENCY% ^(default: 1^)
echo   MIN_UPLOAD_DELAY: %MIN_UPLOAD_DELAY% ^(default: 0^)
echo   MAX_UPLOAD_DELAY: %MAX_UPLOAD_DELAY% ^(default: 0^)
echo   INTER_UPLOAD_DELAY: %INTER_UPLOAD_DELAY% ^(default: 0.5^)
echo.

echo Running bot with diagnostics enabled...
echo.

setlocal enabledelayedexpansion

REM Apply default optimized settings if not set
if not defined DOWNLOAD_CONCURRENCY set DOWNLOAD_CONCURRENCY=8
if not defined UPLOAD_FILE_CONCURRENCY set UPLOAD_FILE_CONCURRENCY=3
if not defined MIN_UPLOAD_DELAY set MIN_UPLOAD_DELAY=0
if not defined MAX_UPLOAD_DELAY set MAX_UPLOAD_DELAY=0
if not defined INTER_UPLOAD_DELAY set INTER_UPLOAD_DELAY=0

echo Applied Configuration:
echo   DOWNLOAD_CONCURRENCY: !DOWNLOAD_CONCURRENCY!
echo   UPLOAD_FILE_CONCURRENCY: !UPLOAD_FILE_CONCURRENCY!
echo   MIN_UPLOAD_DELAY: !MIN_UPLOAD_DELAY!
echo   MAX_UPLOAD_DELAY: !MAX_UPLOAD_DELAY!
echo   INTER_UPLOAD_DELAY: !INTER_UPLOAD_DELAY!
echo.

python main.py

endlocal
pause
