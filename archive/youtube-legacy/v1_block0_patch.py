# 原来：if yt_dlp.version.__version__ < '2025':
# 替换为↓（数字比较，防止字符串比较误判 2026.x < 2025）
try:    _ver_major = int(yt_dlp.version.__version__.split('.')[0])
except: _ver_major = 0
if _ver_major < 2025:
    _pip('yt-dlp')
    import importlib; importlib.reload(yt_dlp)