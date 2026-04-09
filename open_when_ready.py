from __future__ import annotations

import sys
import time
import urllib.request
import webbrowser

URL = "http://127.0.0.1:8765"
TIMEOUT_SECONDS = 90
SLEEP_SECONDS = 0.5


def is_ready() -> bool:
    try:
        with urllib.request.urlopen(URL, timeout=2) as resp:
            return 200 <= getattr(resp, "status", 0) < 500
    except Exception:
        return False


if __name__ == "__main__":
    deadline = time.time() + TIMEOUT_SECONDS
    while time.time() < deadline:
        if is_ready():
            webbrowser.open(URL)
            print(f"[INFO] Browser opened: {URL}", flush=True)
            sys.exit(0)
        time.sleep(SLEEP_SECONDS)
    print(f"[WARN] Server did not become ready within {TIMEOUT_SECONDS} seconds.", flush=True)
    sys.exit(1)
