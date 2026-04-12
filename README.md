# YouTube

快速入口：
- `run_v331.py`（不依赖 `__file__`/`runpy` 的入口）
- 实际主文件：`youtube/v1_yt_downloader_v331.py`（单文件核心逻辑，可直接在 Colab 单元格运行）

Cookie 约定：
- 默认路径：`/content/drive/MyDrive/YouTube_Cookies/youtube_cookies.txt`
- 若将 Cookie 输入为文件夹（如 `/content/drive/MyDrive/YouTube_Cookies/`），程序会自动创建该目录，并自动选择该目录中最新的 `*.txt` Cookie 文件
- 状态文件可见目录：`/content/drive/MyDrive/YouTube_Cookies/metadata/`
  - `yt_state.json`
  - `yt_index.json`
