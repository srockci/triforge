"""Triforge Server — Windows System Tray Launcher.

Controls the uvicorn server process from the system tray.
Start/Stop/Status commands. Logs to logs/tray.log.

Build EXE:
    pip install pyinstaller pystray Pillow
    pyinstaller --onefile --noconsole --name TriforgeTray tray\tray.py
"""
import io
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pystray
from PIL import Image, ImageDraw

PROJECT_DIR = Path(__file__).resolve().parent.parent
VENV_PYTHON = PROJECT_DIR / ".venv" / "Scripts" / "python.exe"
LOG_DIR = PROJECT_DIR / "logs"
SERVER_LOG = LOG_DIR / "server.log"
TRAY_LOG = LOG_DIR / "tray.log"
HOST = "0.0.0.0"
PORT = "8000"


def log(msg: str):
    LOG_DIR.mkdir(exist_ok=True)
    t = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(str(TRAY_LOG), "a", encoding="utf-8") as f:
        f.write(f"[{t}] {msg}\n")


def _make_icon():
    img = Image.new("RGBA", (64, 64), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    draw.ellipse([4, 4, 60, 60], fill=(0, 120, 212, 255))
    draw.polygon([(20, 48), (32, 16), (44, 48)], fill=(255, 255, 255, 255))
    draw.rectangle([28, 28, 36, 42], fill=(0, 120, 212, 255))
    return img


class TrayApp:
    def __init__(self):
        self._proc: subprocess.Popen | None = None
        self._menu_start = pystray.MenuItem("Start", self._on_start, default=True)
        self._menu_stop = pystray.MenuItem("Stop", self._on_stop)
        self._menu_status = pystray.MenuItem("Status", self._on_status)
        icon_img = _make_icon()
        self._icon = pystray.Icon(
            "triforge",
            icon_img,
            "Triforge Server",
            pystray.Menu(
                self._menu_start,
                self._menu_stop,
                pystray.Menu.SEPARATOR,
                self._menu_status,
                pystray.Menu.SEPARATOR,
                pystray.MenuItem("Quit", self._on_quit),
            ),
        )

    def _is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def _on_start(self):
        if self._is_running():
            self._icon.notify("Server already running", "Triforge")
            return
        log("Starting server...")
        try:
            self._proc = subprocess.Popen(
                [
                    str(VENV_PYTHON),
                    "-X", "utf8",
                    "-m", "uvicorn",
                    "triforge_server.server:app",
                    "--host", HOST,
                    "--port", PORT,
                    "--log-level", "info",
                ],
                cwd=str(PROJECT_DIR),
                stdout=open(str(SERVER_LOG), "a", encoding="utf-8"),
                stderr=subprocess.STDOUT,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
            time.sleep(1)
            if self._is_running():
                self._icon.notify(f"Server started on {HOST}:{PORT}", "Triforge")
            else:
                self._icon.notify("Server failed to start (check logs)", "Triforge Error")
        except Exception as e:
            log(f"Start failed: {e}")
            self._icon.notify(str(e), "Triforge Error")

    def _on_stop(self):
        if not self._is_running():
            self._icon.notify("Server not running", "Triforge")
            return
        log("Stopping server...")
        try:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait(timeout=3)
            self._proc = None
            self._icon.notify("Server stopped", "Triforge")
        except Exception as e:
            log(f"Stop failed: {e}")

    def _on_status(self):
        if self._is_running():
            self._icon.notify(f"RUNNING on {HOST}:{PORT}", "Triforge Status")
        else:
            self._icon.notify("STOPPED", "Triforge Status")

    def _on_quit(self):
        log("Quitting tray app...")
        self._on_stop()
        self._icon.stop()

    def run(self):
        log("Tray app started")
        self._icon.run()


if __name__ == "__main__":
    TrayApp().run()
