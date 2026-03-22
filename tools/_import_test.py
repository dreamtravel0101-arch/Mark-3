import sys
sys.path.append(r'D:\cantainer\Telegram_Master - Copy (2)')
try:
    import main
    print('IMPORT OK')
except Exception as e:
    print('IMPORT ERROR:', type(e), e)
    raise
