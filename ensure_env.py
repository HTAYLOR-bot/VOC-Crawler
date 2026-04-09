from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
REQ = APP_DIR / 'requirements.txt'

MODULES = [
    ('flask', 'Flask'),
    ('playwright', 'playwright'),
    ('dateutil', 'python-dateutil'),
]


def run(cmd: list[str]) -> int:
    print('[INFO]', ' '.join(cmd), flush=True)
    return subprocess.call(cmd)


def missing_modules() -> list[str]:
    missing = []
    for module_name, package_name in MODULES:
        try:
            importlib.import_module(module_name)
        except Exception:
            missing.append(package_name)
    return missing


def main() -> int:
    missing = missing_modules()
    if not missing:
        print('[INFO] Python packages already available.', flush=True)
    else:
        print('[WARN] Missing packages detected:', ', '.join(missing), flush=True)
        if run([sys.executable, '-m', 'pip', 'install', '--upgrade', 'pip']) != 0:
            print('[ERROR] pip upgrade failed.', flush=True)
            return 1
        if run([sys.executable, '-m', 'pip', 'install', '-r', str(REQ)]) != 0:
            print('[ERROR] requirements installation failed.', flush=True)
            return 1

    # Always ensure Chromium exists; this is safe to re-run.
    if run([sys.executable, '-m', 'playwright', 'install', 'chromium']) != 0:
        print('[ERROR] Playwright Chromium installation failed.', flush=True)
        return 1

    # Final import check after install.
    missing = missing_modules()
    if missing:
        print('[ERROR] Still missing packages after install:', ', '.join(missing), flush=True)
        return 1

    print('[INFO] Environment check passed.', flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
