from pathlib import Path


def sniff_video_extension(path: Path) -> str:
    try:
        p = Path(path)
        with p.open('rb') as f:
            data = f.read(8192)

        if b'ftyp' in data[:4096]:
            return '.mp4'

        if data.startswith(b'\x1A\x45\xDF\xA3'):
            if b'webm' in data.lower():
                return '.webm'
            return '.mkv'

        if data.startswith(b'RIFF') and b'AVI ' in data[:64]:
            return '.avi'

        if data and data[0] == 0x47:
            return '.ts'

        return 'none'
    except Exception as e:
        return f'error:{e}'


TEST_DIR = Path('tools/tmp_sniff')
TEST_DIR.mkdir(parents=True, exist_ok=True)

samples = {
    'mp4.bin': b'\x00\x00\x00\x18ftypmp42' + b'\x00' * 100,
    'mkv.bin': b'\x1A\x45\xDF\xA3\x00\x00' + b'\x00' * 100,
    'webm.bin': b'\x1A\x45\xDF\xA3webm' + b'\x00' * 100,
    'avi.bin': b'RIFF\x24\x00\x00\x00AVI ' + b'\x00' * 100,
    'ts.bin': b'\x47\x40\x00\x10' + b'\x00' * 100,
    'txt.bin': b'Hello, this is not a video' + b'\x00' * 100,
}

print('Writing sample files...')
for name, data in samples.items():
    p = TEST_DIR / name
    p.write_bytes(data)

print('Running sniff tests:')
for name in samples.keys():
    p = TEST_DIR / name
    print(f'{name}: {sniff_video_extension(p)}')

# cleanup is left for manual inspection
print('Done.')
