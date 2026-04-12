"""Root entrypoint for v331.
Run this file to launch the latest dashboard without browsing subfolders.
"""
from pathlib import Path
import runpy

ROOT = Path(__file__).resolve().parent
TARGET = ROOT / "youtube" / "v1_yt_downloader_v331.py"
runpy.run_path(str(TARGET), run_name="__main__")

