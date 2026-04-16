# YouTube

快速入口：
- 根目录主文件：`yt_downloader.py`（唯一推荐入口）

归档目录：
- `archive/root-legacy/`：历史根目录入口文件
- `archive/youtube-legacy/`：历史版本脚本与模块

Cookie 约定：
- 默认路径：`/content/drive/MyDrive/YouTube_Cookies/youtube_cookies.txt`
- 若将 Cookie 输入为文件夹（如 `/content/drive/MyDrive/YouTube_Cookies/`），程序会自动创建该目录，并自动选择该目录中最新的 `*.txt` Cookie 文件
- 状态文件可见目录：`/content/drive/MyDrive/YouTube_Cookies/metadata/`
  - `yt_state.json`
  - `yt_index.json`
