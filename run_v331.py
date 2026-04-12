"""Notebook-safe entrypoint for v331.
Avoids __file__/runpy so it can be pasted into Colab cells directly.
"""
import importlib

from youtube import v1_yt_downloader_v331 as _v331

importlib.reload(_v331)
