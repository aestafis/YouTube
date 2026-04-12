# ══════════════════════════════════════════════════════════════
# BLOCK 0 ── 依赖
# ══════════════════════════════════════════════════════════════
import subprocess, sys, os, re, json, time
import shutil, traceback, difflib, threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FT
from datetime import datetime

def _pip(*pkgs):
    for p in pkgs:
        subprocess.run([sys.executable,'-m','pip','install','-q','--upgrade',p],
                       check=False, capture_output=True)
def _apt(*pkgs):
    for p in pkgs:
        subprocess.run(['apt-get','install','-y','-q',p],
                       check=False, capture_output=True)

try:
    import yt_dlp
    try:    _ver = int(yt_dlp.version.__version__.split('.')[0])
    except: _ver = 0
    if _ver < 2025: _pip('yt-dlp'); import importlib; importlib.reload(yt_dlp)
    print(f'[OK] yt-dlp {yt_dlp.version.__version__}')
except ImportError:
    _pip('yt-dlp'); import yt_dlp

try:    import yt_dlp_ejs
except: _pip('yt-dlp-ejs')

def _ensure_deno():
    def _try():
        try: return subprocess.run(['deno','--version'],capture_output=True,
                                   text=True,timeout=8).returncode==0
        except: return False
    if _try(): return
    for d in ['/root/.deno/bin',os.path.expanduser('~/.deno/bin')]:
        if os.path.isfile(os.path.join(d,'deno')):
            os.environ['PATH']=d+':'+os.environ.get('PATH','')
            if _try(): return
    try:
        subprocess.run('curl -fsSL https://deno.land/install.sh|sh',
                       shell=True,timeout=120,capture_output=True)
        os.environ['PATH']='/root/.deno/bin:'+os.environ.get('PATH','')
    except: pass
_ensure_deno()

try:    subprocess.run(['ffmpeg','-version'],capture_output=True,timeout=5)
except: _apt('ffmpeg')

try:
    import ipywidgets as W
    from IPython.display import display, HTML
except ImportError:
    _pip('ipywidgets')
    import ipywidgets as W
    from IPython.display import display, HTML

try:    from google.colab import drive as _gdrive; _IN_COLAB=True
except: _IN_COLAB=False

from yt_dlp.utils import DownloadError, MaxDownloadsReached
print('[OK] 所有依赖就绪')


# ══════════════════════════════════════════════════════════════
# BLOCK 1 ── 配置
# ★ v323: outtmpl 加 id 前缀，封面嵌入，字幕开关
# ══════════════════════════════════════════════════════════════
class Cfg:
    COOKIE   = '/content/drive/MyDrive/youtube_cookies.txt'
    SAVE_DIR = '/content/drive/MyDrive/YouTube_Downloads'
    TMP_DIR  = '/content/local_temp'
    STATE    = '/content/drive/MyDrive/.yt_state.json'
    FRAGS    = 16
    DEDUP    = 0.82
    MAX_MB   = 500
    HTTP_CHUNK_MB         = 10
    SEARCH_SOCKET_TIMEOUT = 10
    SEARCH_HARD_TIMEOUT   = 40
    SEARCH_FALLBACK_RATIO = 0.6

    @staticmethod
    def fix(p):
        p = os.path.expanduser(p)
        if p.startswith('/root/drive/'):
            p = '/content/drive/' + p[len('/root/drive/'):]
        return p

SORT_OPTS = {'相关性':'', '最新上传':'date', '最多播放':'viewcount'}
_SP = {'date':'&sp=CAI%3D', 'viewcount':'&sp=CAM%3D', '':''}

MODES = {
    '热门':  {'desc':'实时热门，按播放量排序',
              'sort':'最多播放','min_dur':0,'max_dur':None,
              'neg_kw':True,'count':15,'color':'#e65100'},
    '高质':  {'desc':'深度长视频（10分钟以上）',
              'sort':'相关性','min_dur':600,'max_dur':None,
              'neg_kw':True,'count':10,'color':'#2e7d32'},
    '短视频':{'desc':'5分钟以内精品短内容',
              'sort':'最多播放','min_dur':0,'max_dur':300,
              'neg_kw':False,'count':20,'color':'#1565c0'},
}

NEG_KW = re.compile(
    r'\b(reaction|reacting|unboxing|shocking|giveaway|prank|'
    r'make money|earn \$|passive income|clickbait|subscribe now)\b', re.I)
AD_KW  = re.compile(
    r'\b(sponsored|advertisement|\bad\b|promo code|'
    r'use code|affiliate|discount code)\b', re.I)

KEYWORD_MODULES = {
    'AI / 机器学习': {
        'AI原理':'transformer explained from scratch',
        '大模型实战':'LLM implementation tutorial 2026',
        'AI Agent':'AI agent build from scratch',
        '论文精读':'AI research paper explained 2026',
        '开源项目':'open source AI tools github 2026',
    },
    '编程 / 教学': {
        'Python实战':'python project tutorial 2026',
        '算法讲解':'algorithm explained visually',
        '系统设计':'system design explained',
        '数学直觉':'math intuition visual explanation',
        '前端开发':'web development tutorial 2026',
    },
    '游戏': {
        '精彩时刻':'game highlights 2026',
        '独立游戏':'indie game review 2026',
        '速通纪录':'speedrun world record 2026',
        '游戏设计':'game design analysis deep dive',
    },
    '动漫 / 二次元': {
        '深度分析':'anime video essay 2025',
        '作画赏析':'anime sakuga breakdown',
        '新番推荐':'best anime 2025 recommendation',
        '漫改对比':'manga vs anime adaptation comparison',
    },
    '娱乐 / 纪录片': {
        '电影预告':'movie trailer 2026',
        '深度纪录片':'documentary full length 2026',
        '喜剧短片':'comedy sketch 2026',
        '近期爆款':'most viral video 2026',
    },
}


# ══════════════════════════════════════════════════════════════
# BLOCK 2 ── 工具函数
# ★ v323: _embed_thumb 新增; _rename_with_index 改用 id 精确匹配
# ══════════════════════════════════════════════════════════════
def _fmt_size(b):
    b = int(b or 0)
    if b >= 1<<30: return f'{b/(1<<30):.2f}GB'
    if b >= 1<<20: return f'{b/(1<<20):.1f}MB'
    if b >= 1<<10: return f'{b/(1<<10):.0f}KB'
    return f'{b}B'

def _fmt_views(n):
    if not n: return None
    n = int(n)
    if n >= 1_000_000: return f'{n/1_000_000:.1f}M'
    if n >= 1_000:     return f'{n/1_000:.0f}K'
    return str(n)

def _fmt_age(s):
    if not s: return None
    try:
        s = str(s)
        dt = (datetime(int(s[:4]),int(s[4:6]),int(s[6:]))
              if len(s)==8 else None)
        if not dt: return s[:10]
        d = (datetime.now()-dt).days
        if d <= 0:  return '今天'
        if d == 1:  return '昨天'
        if d < 7:   return f'{d}天前'
        if d < 31:  return f'{d//7}周前'
        if d < 365: return f'{d//30}个月前'
        return f'{d//365}年前'
    except: return str(s)[:10]

def _trim(t, mx=36):
    if not t: return ''
    w = 0
    for i, c in enumerate(t):
        w += 2 if ord(c) > 0x2E7F else 1
        if w > mx: return t[:i] + '...'
    return t

def _dedup(title, seen, thr=Cfg.DEDUP):
    nt = re.sub(r'[^\w\u4e00-\u9fff]', '', (title or '').lower())
    if not nt: return False
    for et in seen[-30:]:
        ne = re.sub(r'[^\w\u4e00-\u9fff]', '', et.lower())
        if difflib.SequenceMatcher(None,nt,ne).ratio() >= thr: return True
    return False

def _make_session_dir(base, mode, query, count):
    now = datetime.now().strftime('%Y%m%d_%H%M')
    m   = re.sub(r'[^\w\u4e00-\u9fff]', '', mode)
    k   = re.sub(r'[^\w\u4e00-\u9fff ]', '', query)[:16].strip()
    k   = re.sub(r'\s+', '_', k) or 'url'
    p   = os.path.join(Cfg.fix(base), f'{now}_{m}_{k}_{count}个')
    os.makedirs(p, exist_ok=True); return p

def _embed_thumb(video_path, thumb_path, log=None):
    """★ v323: 将封面嵌入 mp4 metadata，成功后删除独立缩略图"""
    if not os.path.exists(thumb_path):
        return False
    ext = os.path.splitext(video_path)[1].lower()
    if ext not in ('.mp4', '.m4v'):
        if log: log.write(f'封面嵌入跳过（{ext} 非 mp4）')
        return False
    tmp = video_path + '._emb_.mp4'
    try:
        r = subprocess.run(
            ['ffmpeg', '-loglevel', 'error', '-y',
             '-i', video_path, '-i', thumb_path,
             '-map', '0', '-map', '1',
             '-c', 'copy', '-disposition:v:1', 'attached_pic', tmp],
            capture_output=True, timeout=90)
        if r.returncode == 0 and os.path.exists(tmp) and os.path.getsize(tmp) > 1024:
            os.replace(tmp, video_path)
            try: os.remove(thumb_path)
            except: pass
            return True
        if os.path.exists(tmp):
            try: os.remove(tmp)
            except: pass
        if log:
            msg = r.stderr.decode('utf-8', errors='ignore')[:120].strip()
            if msg: log.write(f'封面嵌入失败: {msg}')
        return False
    except Exception as e:
        if os.path.exists(tmp):
            try: os.remove(tmp)
            except: pass
        if log: log.write(f'封面嵌入异常: {type(e).__name__}: {e}')
        return False

def _rename_with_index(session_dir, vid_order):
    """★ v323: 改用 video_id 前缀精确匹配；空 vid 跳过"""
    for idx, (vid, title) in enumerate(vid_order, 1):
        if not vid: continue                          # URL 下载无 id，跳过
        safe   = re.sub(r'[\\/:*?"<>|]', '_', title)[:40]
        prefix = f'{idx:02d}_'
        id_pfx = vid + '__'
        try:
            for fn in list(os.listdir(session_dir)):
                if fn.startswith('README'): continue
                if fn.startswith(id_pfx) and not fn.startswith(prefix):
                    src  = os.path.join(session_dir, fn)
                    rest = fn[len(id_pfx):]           # 去掉 "XXXX__" 前缀
                    ext  = os.path.splitext(rest)[1]
                    dst  = os.path.join(session_dir, f'{prefix}{safe}{ext}')
                    try: os.rename(src, dst)
                    except: pass
        except: pass

def _write_index(sd, mode, query, sort, items, done_ids):
    lines = ['=== YouTube 下载索引 ===',
             f'时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
             f'关键词: {query}  模式: {mode}  排序: {sort}',
             f'计划: {len(items)}  完成: {len(done_ids)}', '',
             '--- 列表（按播放量排序）---']
    for i, x in enumerate(items, 1):
        ok = 'OK' if x.get('id','') in done_ids else '--'
        v  = _fmt_views(x.get('view_count')) or '-'
        a  = _fmt_age(x.get('upload_date','')) or '-'
        lines += [f'[{ok}] {i:02d}. [{v}|{a}] {x.get("title","")[:55]}',
                  f'      {x.get("channel","N/A")}',
                  f'      {x.get("url","")}', '']
    p = os.path.join(sd, 'README_index.txt')
    with open(p, 'w', encoding='utf-8') as f: f.write('\n'.join(lines))
    return p


# ══════════════════════════════════════════════════════════════
# BLOCK 3 ── UIQueue