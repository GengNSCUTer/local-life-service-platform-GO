#!/usr/bin/env python3
"""Build the adapted web assets from the original course archive."""
import argparse
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import tempfile
from zipfile import ZipFile


REPO = Path(__file__).resolve().parents[1]
WEB_ROOT = 'nginx-1.18.0/html/hmdp/'
PATCHED_FILES = ('index.html', 'blog-detail.html', 'info.html', 'js/common.js')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('archive', type=Path, help='Path to the original nginx-1.18.0.zip')
    parser.add_argument('output', type=Path, help='New directory for the adapted web assets')
    args = parser.parse_args()
    if args.output.exists():
        parser.error(f'output already exists: {args.output}')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=args.output.parent) as work:
        web = Path(work) / 'web'
        with ZipFile(args.archive) as archive:
            for entry in archive.infolist():
                if entry.is_dir() or not entry.filename.startswith(WEB_ROOT):
                    continue
                relative = PurePosixPath(entry.filename.removeprefix(WEB_ROOT))
                if relative.is_absolute() or any(part.startswith('.') for part in relative.parts):
                    continue
                file = web / relative
                file.parent.mkdir(parents=True, exist_ok=True)
                file.write_bytes(archive.read(entry))
        for name in PATCHED_FILES:
            file = web / name
            file.write_bytes(file.read_bytes().replace(b'\r\n', b'\n'))
        subprocess.run(['patch', '--batch', '--forward', '--no-backup-if-mismatch', '--fuzz=0', '-p1',
                        '-i', str(REPO / 'frontend/blog-like.patch')], cwd=web, check=True)
        shutil.copyfile(REPO / 'frontend/blog-like.js', web / 'js/blog-like.js')
        web.rename(args.output)


if __name__ == '__main__':
    main()
