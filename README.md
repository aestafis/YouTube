# YouTube

快速入口：
- `run_v331.py`（不依赖 `__file__`/`runpy` 的入口）
- 根目录主文件：`yt_downloader_v331_final.py`（推荐直接使用）
- 同步源码：`youtube/v1_yt_downloader_v331.py`

Cookie 约定：
- 默认路径：`/content/drive/MyDrive/YouTube_Cookies/youtube_cookies.txt`
- 若将 Cookie 输入为文件夹（如 `/content/drive/MyDrive/YouTube_Cookies/`），程序会自动创建该目录，并自动选择该目录中最新的 `*.txt` Cookie 文件
- 状态文件可见目录：`/content/drive/MyDrive/YouTube_Cookies/metadata/`
  - `yt_state.json`
  - `yt_index.json`
