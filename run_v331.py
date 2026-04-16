"""Notebook-safe entrypoint for v331.
Avoids __file__/runpy so it can be pasted into Colab cells directly.
"""
import importlib

import yt_downloader_v331_final as _v331

_v331 = importlib.reload(_v331)
