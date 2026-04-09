from __future__ import annotations

import socket
import sys
import traceback
from pathlib import Path

LOG_PATH = Path(__file__).resolve().parent / "server_boot.log"


def log(msg: str) -> None:
    line = f"{msg}\n"
    try:
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass
    print(msg, flush=True)


def is_port_in_use(host: str, port: int) -> bool:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1.0)
    try:
        return s.connect_ex((host, port)) == 0
    finally:
        s.close()


if __name__ == "__main__":
    host = "127.0.0.1"
    port = 8765
    try:
        if is_port_in_use(host, port):
            log(f"[ERROR] {host}:{port} is already in use.")
            log("Close the other process using that port, then run again.")
            sys.exit(1)

        log("[INFO] Importing Flask app...")
        from app import app  # noqa: WPS433

        log(f"[INFO] Starting server at http://{host}:{port}")
        app.run(host=host, port=port, debug=False, use_reloader=False)
    except Exception as exc:
        log(f"[ERROR] Server failed to start: {exc}")
        tb = traceback.format_exc()
        try:
            with LOG_PATH.open("a", encoding="utf-8") as f:
                f.write(tb + "\n")
        except Exception:
            pass
        print(tb, flush=True)
        sys.exit(1)
