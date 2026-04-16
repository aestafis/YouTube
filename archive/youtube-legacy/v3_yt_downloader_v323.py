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
    """封面嵌入 mp4 metadata，成功后删除独立缩略图"""
    if not os.path.exists(thumb_path):
        return False
    if os.path.splitext(video_path)[1].lower() not in ('.mp4', '.m4v'):
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
    """改用 video_id 前缀精确匹配；空 vid 跳过"""
    for idx, (vid, title) in enumerate(vid_order, 1):
        if not vid: continue
        safe   = re.sub(r'[\\/:*?"<>|]', '_', title)[:40]
        prefix = f'{idx:02d}_'
        id_pfx = vid + '__'
        try:
            for fn in list(os.listdir(session_dir)):
                if fn.startswith('README'): continue
                if fn.startswith(id_pfx) and not fn.startswith(prefix):
                    src = os.path.join(session_dir, fn)
                    ext = os.path.splitext(fn[len(id_pfx):])[1]
                    dst = os.path.join(session_dir, f'{prefix}{safe}{ext}')
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
# BLOCK 3 ── UIQueue + StatusBar
# ══════════════════════════════════════════════════════════════
class _UIQueue:
    def __init__(self):
        self._lock      = threading.Lock()
        self._pending   = {}
        self._callbacks = []

    def put(self, kind, value):
        with self._lock: self._pending[kind] = value

    def put_cb(self, fn):
        with self._lock: self._callbacks.append(fn)

    def drain(self):
        with self._lock:
            p = dict(self._pending)
            c = list(self._callbacks)
            self._pending.clear()
            self._callbacks.clear()
        return p, c


class StatusBar:
    _TMPL = (
        '<div style="font-size:13px;font-family:monospace;'
        'background:{bg};color:{fg};padding:7px 14px;'
        'border-radius:4px;border:1px solid {border};'
        'line-height:1.8;margin:2px 0;'
        'white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'
        '{icon}&nbsp;&nbsp;{msg}</div>')

    def __init__(self, uiq):
        self._uiq = uiq
        self._w   = W.HTML(
            value=self._r('⬜','#f5f5f5','#888','#ddd','待机中'),
            layout=W.Layout(width='100%'))

    def _r(self, icon, bg, fg, border, msg):
        return self._TMPL.format(bg=bg,fg=fg,border=border,icon=icon,msg=msg)
    def _push(self, html): self._uiq.put('status', html)

    def idle(self):
        self._push(self._r('⬜','#f5f5f5','#888','#ddd','待机中'))
    def searching(self, q):
        self._push(self._r('🔍','#e3f2fd','#1565c0','#90caf9',
                           f'正在搜索: {_trim(q,40)}'))
    def found(self, n, mode):
        self._push(self._r('✅','#e8f5e9','#2e7d32','#a5d6a7',
                           f'找到 {n} 个视频（{mode}），勾选后点"开始下载"'))
    def downloading(self, idx, n, title, overall_pct=None):
        prog = f'[总{int(overall_pct):3d}%] ' if overall_pct is not None else ''
        self._push(self._r('⬇','#fff8e1','#e65100','#ffcc02',
                           f'{prog}下载中 {idx}/{n} | {_trim(title,38)}'))
    def update_progress(self, pct, idx, n, title, overall_pct=None):
        prog = f'[总{int(overall_pct):3d}%] ' if overall_pct is not None else ''
        self._push(self._r('⬇','#fff8e1','#e65100','#ffcc02',
                           f'{prog}{int(pct)}% {idx}/{n} | {_trim(title,38)}'))
    def paused_after_video(self, idx, n):
        self._push(self._r('⏸','#fce4ec','#c62828','#ef9a9a',
                           f'已暂停 ({idx}/{n} 完成) · 点"继续"开始下一个'))
    def paused(self, idx, n):
        self._push(self._r('⏸','#fce4ec','#c62828','#ef9a9a',
                           f'暂停中… 当前视频下完后停止 ({idx}/{n})'))
    def resuming(self):
        self._push(self._r('▶','#e8f5e9','#2e7d32','#a5d6a7','继续下载中...'))
    def done(self, done, fails, size, elapsed):
        self._push(self._r('🎉','#e8f5e9','#2e7d32','#a5d6a7',
                           f'完成 ✓{done} ✗{fails} {_fmt_size(size)} {elapsed:.0f}s'))
    def stopped(self, done, fails):
        self._push(self._r('⏹','#f5f5f5','#888','#ddd',
                           f'已停止 ✓{done} ✗{fails}'))
    def error(self, msg):
        self._push(self._r('❌','#fce4ec','#c62828','#ef9a9a', msg))
    def widget(self): return self._w


# ══════════════════════════════════════════════════════════════
# BLOCK 4 ── LiveLog
# ══════════════════════════════════════════════════════════════
class LiveLog:
    _MAX = 200

    def __init__(self, uiq):
        self._uiq   = uiq
        self._w     = W.Textarea(
            value='', disabled=True,
            placeholder='下载日志将在此显示...',
            layout=W.Layout(width='100%', height='220px'))
        self._lock  = threading.Lock()
        self._lines = []

    def write(self, msg):
        ts = datetime.now().strftime('%H:%M:%S')
        line = f'[{ts}] {msg}'
        with self._lock:
            self._lines.append(line)
            if len(self._lines) > self._MAX:
                self._lines = self._lines[-self._MAX:]
            text = '\n'.join(self._lines)
        self._uiq.put('log', text)

    def clear(self):
        with self._lock: self._lines = []
        self._uiq.put('log', '')

    def widget(self): return self._w


# ══════════════════════════════════════════════════════════════
# BLOCK 5 ── 持久化状态
# ══════════════════════════════════════════════════════════════
class State:
    def __init__(self):
        self._p  = Cfg.fix(Cfg.STATE)
        self._dl = set(); self._fail = {}
        self._load()

    def _load(self):
        try:
            with open(self._p, encoding='utf-8') as f: d = json.load(f)
            self._dl   = set(d.get('downloaded', []))
            self._fail = d.get('failed', {})
        except: pass

    def _save(self):
        try:
            os.makedirs(os.path.dirname(self._p), exist_ok=True)
            tmp = self._p + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump({'downloaded':list(self._dl), 'failed':self._fail,
                           'updated':datetime.now().isoformat()},
                          f, ensure_ascii=False, indent=2)
            os.replace(tmp, self._p)
        except: pass

    def done(self, v):      self._dl.add(v); self._fail.pop(v, None); self._save()
    def fail(self, v, t, r):
        x = self._fail.setdefault(v, {'title':t, 'count':0})
        x['reason'] = r; x['count'] += 1
    def is_done(self, v):   return v in self._dl
    def can_retry(self, v): return self._fail.get(v, {}).get('count', 0) < 3
    def reset(self):        self._dl.clear(); self._fail.clear(); self._save()


# ══════════════════════════════════════════════════════════════
# BLOCK 6 ── Cookie + Drive
# ══════════════════════════════════════════════════════════════
class CookieError(Exception): pass

def _mount_drive():
    if os.path.ismount('/content/drive'): return True, '已挂载'
    if not _IN_COLAB: return False, '非Colab'
    try:
        _gdrive.mount('/content/drive')
        return os.path.ismount('/content/drive'), '挂载成功'
    except Exception as e: return False, str(e)

def _check_cookie(path):
    path = Cfg.fix(path)
    if not os.path.exists(path): raise CookieError(f'不存在:{path}')
    if os.path.getsize(path) == 0: raise CookieError('文件为空')
    with open(path, encoding='utf-8', errors='ignore') as f: c = f.read(256*1024)
    if 'youtube.com' not in c: raise CookieError('无youtube.com条目')
    found = [k for k in ('SAPISID','__Secure-1PSID','LOGIN_INFO') if k in c]
    if not found: raise CookieError('未找到登录信息')
    return found


# ══════════════════════════════════════════════════════════════
# BLOCK 7 ── 搜索
# ══════════════════════════════════════════════════════════════
def _build_url(query, sort_key):
    q = query.strip()
    if not q: return None
    if q.startswith(('http://','https://')): return q
    return (f'https://www.youtube.com/results'
            f'?search_query={q.replace(" ","+")}'
            f'{_SP.get(sort_key,"")}')

def _do_search_raw(url, pool_size, cookie_path, mode_cfg):
    opts = {'quiet':True,'no_warnings':True,'extract_flat':'in_playlist',
            'skip_download':True,'cookiefile':Cfg.fix(cookie_path),
            'ignoreerrors':True,'no_check_certificates':True,
            'socket_timeout':Cfg.SEARCH_SOCKET_TIMEOUT,
            'playlistend':pool_size}
    info = None
    def _ex():
        with yt_dlp.YoutubeDL(opts) as y:
            return y.extract_info(url, download=False)
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            try: info = ex.submit(_ex).result(timeout=Cfg.SEARCH_HARD_TIMEOUT)
            except FT: return []
    except MaxDownloadsReached: pass
    except DownloadError as e:
        if any(x in str(e) for x in ('403','Forbidden','Sign in')):
            raise CookieError('Cookie已失效(403)') from e
        return []
    except Exception: return []
    if not info: return []
    entries = (info.get('entries')
               if info.get('_type') in ('playlist','search') else [info])
    return entries or []

def _filter_entries(entries, count, mode_cfg):
    res = []; seen_ids = set(); seen_t = []
    for e in entries:
        if len(res) >= count: break
        if not e or not isinstance(e, dict): continue
        vid   = e.get('id',''); title = (e.get('title','') or '').strip()
        if not vid or not title or vid in seen_ids: continue
        ds = int(e.get('duration') or 0)
        if AD_KW.search(title): continue
        if mode_cfg.get('neg_kw') and NEG_KW.search(title): continue
        if _dedup(title, seen_t): continue
        mn = mode_cfg.get('min_dur', 0); mx = mode_cfg.get('max_dur')
        if ds > 0:
            if mn and ds < mn: continue
            if mx and ds > mx: continue
        seen_ids.add(vid); seen_t.append(title)
        u = e.get('url','') or e.get('webpage_url','')
        if u and not u.startswith('http'):
            u = f'https://www.youtube.com/watch?v={u}'
        if not u and vid:
            u = f'https://www.youtube.com/watch?v={vid}'
        dur = e.get('duration_string','')
        if not dur and ds: dur = f'{int(ds)//60}:{int(ds)%60:02d}'
        res.append({'id':vid, 'title':title,
                    'channel':e.get('channel') or e.get('uploader','N/A'),
                    'url':u, 'duration':dur or 'N/A', 'dur_s':ds,
                    'view_count':e.get('view_count'),
                    'upload_date':e.get('upload_date','')})
    res.sort(key=lambda x: int(x.get('view_count') or 0), reverse=True)
    return res

def _do_search(url, count, cookie_path, mode_cfg):
    entries = _do_search_raw(url, count * 4, cookie_path, mode_cfg)
    res = _filter_entries(entries, count, mode_cfg)
    if len(res) < count * Cfg.SEARCH_FALLBACK_RATIO:
        entries2 = _do_search_raw(url, count * 6, cookie_path, mode_cfg)
        res = _filter_entries(entries2, count, mode_cfg)
    return res


# ════════════════════════���═════════════════════════════════════
# BLOCK 8 ── 下载
# ★ v323:
#   - outtmpl 加 %(id)s__ 前缀，解决文件匹配失误
#   - 视频/附属文件分类判定（main_ok）
#   - 封面自动嵌入 mp4
#   - subtitle_on 开关
#   - table_mark_cb 行状态回调
#   - overall_pct 整体进度
# ══════════════════════════════════════════════════════════════
class _StopDownload(Exception): pass

_WARN_SKIP = ('PO Token','po_token','subtitles require','missing subtitles',
              'jsc','SABR','Ignoring unsupported')
_SB_RE = re.compile(
    r'captcha|not a robot|automated.{0,30}access|unusual traffic|'
    r'please verify|sign in to confirm|too many request|'
    r'rate.?limit|http error 429|http error 403', re.I)

_VIDEO_EXTS = frozenset({'.mp4','.mkv','.webm','.m4v','.avi','.flv'})
_THUMB_EXTS = frozenset({'.webp','.jpg','.jpeg','.png'})
_SUB_EXTS   = frozenset({'.vtt','.srt','.ass','.ssa'})


def _do_download(items, cookie_path, save_dir,
                 stop_ev, pause_ev, state, session_dir,
                 log, status, progress_cb=None, flush_cb=None,
                 subtitle_on=False, table_mark_cb=None):

    def _mark(vid, s, r=''):
        if table_mark_cb and vid:
            try: table_mark_cb(vid, s, r)
            except: pass

    cp = Cfg.fix(cookie_path)
    os.makedirs(Cfg.TMP_DIR, exist_ok=True)
    os.makedirs(session_dir, exist_ok=True)
    done = fails = 0
    stop_why = None
    total_bytes = 0
    t0 = time.time()
    sb_consec = 0
    n = len(items)
    done_ids = set()
    vid_order = []

    log.write(f'开始下载 {n} 个视频  [FRAGS={Cfg.FRAGS} chunk={Cfg.HTTP_CHUNK_MB}MB '
              f'字幕={"开" if subtitle_on else "关"}]')
    log.write(f'目录: {session_dir}')
    if flush_cb: flush_cb()

    for idx, item in enumerate(items, 1):
        if stop_ev.is_set():
            stop_why = 'user_stop'; break

        vid   = item.get('id', '')
        title = item.get('title', '')[:52]
        url   = item.get('url', '')
        views = _fmt_views(item.get('view_count')) or '-'

        overall_start = (idx - 1) / n * 100
        if status: status.downloading(idx, n, title, overall_pct=overall_start)
        if progress_cb: progress_cb(idx, n, title)
        _mark(vid, 'downloading')
        if flush_cb: flush_cb()

        if vid and state.is_done(vid):
            log.write(f'[{idx}/{n}] 跳过(已下载): {title}')
            _mark(vid, 'skip', '已下载')
            if flush_cb: flush_cb()
            continue
        if vid and not state.can_retry(vid):
            log.write(f'[{idx}/{n}] 跳过(多次失败): {title}')
            _mark(vid, 'fail', '多次失败')
            fails += 1
            if flush_cb: flush_cb()
            continue

        log.write(f'[{idx}/{n}] {title} | {views} | {item.get("duration","N/A")}')

        shutil.rmtree(Cfg.TMP_DIR, ignore_errors=True)
        os.makedirs(Cfg.TMP_DIR)
        sb_log = []
        last_step = [-1]
        last_flush_pct = [-1]

        class _Logger:
            def debug(self_, msg):
                if stop_ev.is_set(): raise _StopDownload()
                if '[download]' in msg:
                    m = re.search(r'(\d+(?:\.\d+)?)%', msg)
                    if m:
                        pct     = float(m.group(1))
                        step    = int(pct)
                        overall = (idx - 1 + pct / 100) / n * 100
                        if step != last_step[0]:
                            last_step[0] = step
                            if status:
                                status.update_progress(pct, idx, n, title,
                                                       overall_pct=overall)
                            if progress_cb: progress_cb(idx, n, f'{int(pct)}%')
                            bucket = (step // 25) * 25
                            if bucket != last_flush_pct[0]:
                                last_flush_pct[0] = bucket
                                if flush_cb: flush_cb()
                if _SB_RE.search(msg):
                    sb_log.append(msg[:80])

            def warning(self_, msg):
                if any(k in msg for k in _WARN_SKIP): return
                if '429' in msg and 'subtitle' in msg.lower():
                    log.write('字幕限速，已跳过'); return
                log.write(f'注意: {msg.strip()[:88]}')

            def error(self_, msg):
                if '429' in msg and 'subtitle' in msg.lower():
                    log.write('字幕失败，已跳过'); return
                log.write(f'错误: {msg.strip()[:88]}')

        opts = {
            'quiet':             False,
            'no_warnings':       False,
            'logger':            _Logger(),
            'cookiefile':        cp,
            'concurrent_fragment_downloads': Cfg.FRAGS,
            'http_chunk_size':   Cfg.HTTP_CHUNK_MB * 1024 * 1024,
            'writethumbnail':    True,
            'writesubtitles':    subtitle_on,
            'writeautomaticsub': subtitle_on,
            'subtitleslangs':    (['zh-Hans','zh-Hant','en'] if subtitle_on else []),
            'ffmpeg_location':   '/usr/bin/ffmpeg',
            'format':            ('bestvideo[ext=mp4][height<=1080]'
                                  '+bestaudio[ext=m4a]/best[ext=mp4]/best'),
            'outtmpl':           f'{Cfg.TMP_DIR}/%(id)s__%(title).60s.%(ext)s',
            'no_check_certificates': True,
            'ignoreerrors':      False,
            'retries':           3,
            'fragment_retries':  5,
            'socket_timeout':    30,
            'sleep_interval_requests': 0.5,
            'max_sleep_interval':      1.2,
            'remote_components': ['ejs:github'],
            'max_filesize':      Cfg.MAX_MB * 1024 * 1024,
            'playlist_items':    '1',
        }

        dl_ok = False
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                dl_ok = (ydl.download([url]) == 0)
        except _StopDownload:
            log.write('已强制中断')
            stop_why = 'user_stop'; break
        except DownloadError as e:
            s = str(e).lower()
            if '403' in s or 'forbidden' in s:
                log.write('Cookie失效，请重新导出')
                if status: status.error('Cookie已失效')
                state.fail(vid, title, '403')
                _mark(vid, 'fail', 'Cookie失效')
                stop_why = '403'
                if flush_cb: flush_cb()
                break
            elif '429' in s and 'subtitle' in s:
                log.write('字幕限速，跳过字幕')
                dl_ok = True
            elif '429' in s:
                log.write('触发限速，等45秒...')
                time.sleep(45)
                state.fail(vid, title, '429'); fails += 1
                _mark(vid, 'fail', '限速429')
                if flush_cb: flush_cb()
                continue
            elif 'no space' in s:
                log.write('Drive磁盘已满')
                if status: status.error('Drive磁盘已满')
                stop_why = 'disk_full'
                if flush_cb: flush_cb()
                break
            else:
                log.write(f'下载失败: {str(e)[:100]}')
                state.fail(vid, title, 'dl_err'); fails += 1
                _mark(vid, 'fail', str(e)[:60])
                if flush_cb: flush_cb()
                continue
        except Exception as e:
            if stop_ev.is_set():
                stop_why = 'user_stop'; break
            log.write(f'意外错误: {type(e).__name__}: {e}')
            state.fail(vid, title, type(e).__name__); fails += 1
            _mark(vid, 'fail', type(e).__name__)
            if flush_cb: flush_cb()
            continue

        if stop_ev.is_set():
            stop_why = 'user_stop'; break

        if sb_log:
            sb_consec += 1
            if sb_consec >= 3:
                log.write('多次访问限制，等90秒...')
                time.sleep(90); sb_consec = 0
        else:
            sb_consec = 0

        if dl_ok:
            tmp_files = []
            try: tmp_files = os.listdir(Cfg.TMP_DIR)
            except: pass

            if not tmp_files:
                log.write(f'无文件(地区限制/超大小上限): {title}')
                state.fail(vid, title, 'empty'); fails += 1
                _mark(vid, 'fail', '无文件')
                if flush_cb: flush_cb()
            else:
                vid_files   = []   # [(fn, dst, sz)]
                thumb_files = []   # [(fn, dst, sz)]
                copy_ok     = True

                for fn in tmp_files:
                    src = os.path.join(Cfg.TMP_DIR, fn)
                    dst = os.path.join(session_dir, fn)
                    ext = os.path.splitext(fn)[1].lower()
                    try:
                        shutil.copy2(src, dst)
                        sz = os.path.getsize(dst)
                        try: os.remove(src)
                        except: pass
                        total_bytes += sz
                        log.write(f'已保存: {fn} ({_fmt_size(sz)})')
                        if ext in _VIDEO_EXTS:
                            vid_files.append((fn, dst, sz))
                        elif ext in _THUMB_EXTS:
                            thumb_files.append((fn, dst, sz))
                    except OSError as e2:
                        log.write(f'保存失败: {fn} - {e2}')
                        if ext in _VIDEO_EXTS:
                            copy_ok = False

                # 主视频判定：有视频文件且 > 100KB
                main_ok = copy_ok and any(sz > 100*1024 for _, _, sz in vid_files)

                # 封面嵌入
                for vfn, vdst, _ in vid_files:
                    v_stem = os.path.splitext(vdst)[0]
                    for entry in list(thumb_files):
                        tfn, tdst, _tsz = entry
                        if os.path.splitext(tdst)[0] == v_stem:
                            if _embed_thumb(vdst, tdst, log):
                                log.write(f'封面已嵌入: {vfn}')
                                thumb_files.remove(entry)
                            break

                if main_ok:
                    state.done(vid); done += 1
                    done_ids.add(vid)
                    vid_order.append((vid, title))
                    log.write(f'✓ 完成 [{done}/{n}]: {title}')
                    _mark(vid, 'done')
                else:
                    log.write(f'✗ 主视频缺失或过小: {title}')
                    state.fail(vid, title, 'video_small'); fails += 1
                    _mark(vid, 'fail', '视频文件过小')
        else:
            log.write(f'✗ 下载失败(yt-dlp返回非0): {title}')
            state.fail(vid, title, 'yt_fail'); fails += 1
            _mark(vid, 'fail', 'yt-dlp返回失败')

        if flush_cb: flush_cb()

        # 视频级暂停：下完一个视频后才响应
        if pause_ev.is_set() and not stop_ev.is_set():
            if status: status.paused_after_video(done, n)
            log.write(f'⏸ 已暂停（{done}/{n} 已完成）· 点"继续"开始下一个')
            if flush_cb: flush_cb()
            while pause_ev.is_set() and not stop_ev.is_set():
                time.sleep(0.5)
            if not stop_ev.is_set():
                if status: status.resuming()
                log.write('▶ 继续下载...')
                if flush_cb: flush_cb()

    # ── 收尾 ──────────────────────────────────────────────────
    try: _rename_with_index(session_dir, vid_order)
    except: pass
    try: state._save()
    except: pass

    elapsed = time.time() - t0
    spd = (_fmt_size(int(total_bytes / elapsed)) + '/s'
           if elapsed > 0 and total_bytes > 0 else '-')
    label = {'403':'Cookie失效中止','disk_full':'磁盘已满中止',
             'user_stop':'已手动停止'}.get(stop_why, '全部完成')
    log.write('=' * 40)
    log.write(f'{label} | ✓{done} ✗{fails} | {_fmt_size(total_bytes)} | {spd} | {elapsed:.0f}s')
    log.write('=' * 40)
    return done, fails, stop_why, done_ids, total_bytes, elapsed


# ══════════════════════════════════════════════════════════════
# BLOCK 9 ── PreviewTable
# ★ v323:
#   - get_selected() 修复三态语义: None/[]/[...]
#   - mark() + apply_pending_marks() 线程安全行状态更新
#   - 去除 DRAG_JS（与 Colab comm 冲突，复选框状态不同步）
# ══════════════════════════════════════════════════════════════
_AUTO_REFRESH_JS = """
<script>
(function(){
  if (window._yt_auto_timer) clearInterval(window._yt_auto_timer);
  window._yt_auto_timer = setInterval(function(){
    try { google.colab.kernel.invokeFunction('_yt_dl_flush', [], {}); } catch(e) {}
  }, 2000);
})();
</script>
"""


class PreviewTable:
    def __init__(self):
        self._items            = []
        self._boxes            = []
        self._row_widgets      = []   # W.HTML per row，用于行状态更新
        self._pending_marks    = {}
        self._marks_lock       = threading.Lock()
        self.container         = W.VBox(layout=W.Layout(width='100%'))

    # ── 行 HTML 生成 ──────────────────────────────────────────
    def _row_html(self, i, r, row_status=None, reason=''):
        title = r.get('title',''); ts  = _trim(title, 34)
        ch    = _trim(r.get('channel') or 'N/A', 18)
        dur   = r.get('duration','N/A')
        url   = r.get('url','#')
        views = _fmt_views(r.get('view_count'))
        age   = _fmt_age(r.get('upload_date',''))

        bg = {'done':'#e8f5e9','fail':'#fce4ec',
              'skip':'#f5f5f5','downloading':'#fff8e1'}.get(
              row_status, '#fff' if i % 2 == 0 else '#f9f9f9')
        icon_map = {'done':'✅','fail':'❌','skip':'⏭','downloading':'⬇'}
        icon = icon_map.get(row_status, str(i + 1))

        tip = (f' title="失败: {reason}"'
               if row_status == 'fail' and reason else '')
        vh  = (f'<span style="font-size:12px;color:#444">{views}</span>'
               if views else '<span style="color:#ddd;font-size:11px">-</span>')
        ah  = (f'<span style="font-size:11px;color:#666">{age}</span>'
               if age else '<span style="color:#ddd;font-size:11px">-</span>')

        return (
            f'<div style="display:grid;'
            f'grid-template-columns:24px 1fr 68px 68px 54px;'
            f'gap:0 4px;align-items:center;min-height:54px;'
            f'padding:3px 4px 3px 0;background:{bg};'
            f'border-bottom:1px solid #eee;user-select:none"{tip}>'
            f'<div style="text-align:center;color:#aaa;font-size:11px">{icon}</div>'
            f'<div style="min-width:0;overflow:hidden;padding-left:4px">'
            f'<a href="{url}" target="_blank" '
            f'style="color:#1a73e8;text-decoration:none;font-size:13px;'
            f'font-weight:500;display:block;line-height:1.5;'
            f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis"'
            f' title="{title}">{ts}</a>'
            f'<div style="color:#888;font-size:11px;white-space:nowrap;'
            f'overflow:hidden;text-overflow:ellipsis">{ch}</div></div>'
            f'<div style="text-align:center">{vh}</div>'
            f'<div style="text-align:center">{ah}</div>'
            f'<div style="text-align:center;font-size:12px;color:#444">{dur}</div>'
            f'</div>'
        )

    # ── 渲染 ──────────────────────────────────────────────────
    def render(self, items):
        self._items       = list(items)
        self._boxes       = []
        self._row_widgets = []
        with self._marks_lock: self._pending_marks.clear()

        if not items:
            self.container.children = (
                W.HTML('<div style="padding:20px;text-align:center;'
                       'color:#999;font-size:13px">未找到结果</div>'),)
            return

        all_cb = W.Checkbox(value=True, description='全选', indent=False,
                            layout=W.Layout(width='60px', min_width='60px'))
        def _toggle(c):
            for b in self._boxes: b.value = c['new']
        all_cb.observe(_toggle, names='value')

        header = W.HTML(
            '<div style="display:grid;'
            'grid-template-columns:24px 1fr 68px 68px 54px;'
            'gap:0 4px;align-items:center;font-size:11px;color:#888;'
            'background:#f2f2f2;padding:5px 4px;'
            'border-bottom:2px solid #ccc;user-select:none">'
            '<div style="text-align:center">#</div>'
            '<div style="padding-left:4px">标题 / 频道</div>'
            '<div style="text-align:center">播放量</div>'
            '<div style="text-align:center">发布时间</div>'
            '<div style="text-align:center">时长</div></div>')

        rows = []
        for i, r in enumerate(self._items):
            cb = W.Checkbox(value=True, description='', indent=False,
                            layout=W.Layout(width='72px', min_width='72px',
                                            height='54px', padding='0 16px'))
            self._boxes.append(cb)

            row_w = W.HTML(value=self._row_html(i, r),
                           layout=W.Layout(flex='1', min_width='0'))
            self._row_widgets.append(row_w)

            rows.append(W.HBox([cb, row_w],
                               layout=W.Layout(width='100%',
                                               align_items='center',
                                               min_height='54px')))

        # 范围选择
        wf = W.BoundedIntText(value=1, min=1, max=len(items), step=1,
                              description='从', style={'description_width':'20px'},
                              layout=W.Layout(width='74px'))
        wt = W.BoundedIntText(value=len(items), min=1, max=len(items), step=1,
                              description='到', style={'description_width':'20px'},
                              layout=W.Layout(width='74px'))
        ws = W.Button(description='勾选', layout=W.Layout(width='56px',height='28px'),
                      style={'font_size':'11px','button_color':'#e8f5e9'})
        wd = W.Button(description='取消', layout=W.Layout(width='56px',height='28px'),
                      style={'font_size':'11px','button_color':'#fce4ec'})
        ws.on_click(lambda _: [setattr(b,'value',True)
                               for b in self._boxes[wf.value-1:wt.value]])
        wd.on_click(lambda _: [setattr(b,'value',False)
                               for b in self._boxes[wf.value-1:wt.value]])
        range_row = W.HBox(
            [W.HTML('<span style="font-size:11px;color:#888;margin:auto 6px">范围:</span>'),
             wf, wt, ws, wd,
             W.HTML('<span style="font-size:10px;color:#aaa;margin:auto 8px">'
                    '（单击单选 · 范围批量选）</span>')],
            layout=W.Layout(align_items='center', margin='3px 0'))

        note = W.HTML(
            f'<div style="font-size:10px;color:#888;padding:4px 8px;'
            f'background:#fafafa;border-bottom:1px solid #eee">'
            f'共 <b>{len(items)}</b> 个 · 已按播放量排序</div>')
        ctrl = W.HBox([all_cb, header],
                      layout=W.Layout(width='100%', align_items='center'))
        self.container.children = tuple([note, ctrl, range_row] + rows)

    # ── 勾选读取（三态） ────────────────────────────────────────
    def get_selected(self):
        """
        返回值含义：
          None  → 尚无预览结果（_items 为空）
          []    → 预览存在但全部取消勾选
          [...] → 正常选中列表
        """
        if not self._items:
            return None
        return [r for r, cb in zip(self._items, self._boxes) if cb.value]

    # ── 下载中行状态（线程安全） ───────────────────────────────
    def mark(self, vid_id, status, reason=''):
        """下载线程调用：排队行状态更新"""
        with self._marks_lock:
            self._pending_marks[vid_id] = (status, reason)

    def apply_pending_marks(self):
        """主线程（flush_queue）调用：将标记应用到行 widget"""
        with self._marks_lock:
            marks = dict(self._pending_marks)
            self._pending_marks.clear()
        if not marks: return
        for vid_id, (st, reason) in marks.items():
            for i, item in enumerate(self._items):
                if item.get('id','') == vid_id and i < len(self._row_widgets):
                    self._row_widgets[i].value = self._row_html(i, item, st, reason)
                    break

    def clear(self):
        self._items = []; self._boxes = []; self._row_widgets = []
        with self._marks_lock: self._pending_marks.clear()
        self.container.children = ()


# ══════════════════════════════════════════════════════════════
# BLOCK 10 ── Dashboard
# ★ v323:
#   - 字幕开关（设置面板，默认关）
#   - _on_download 修复 None/[] 勾选语义（核心 bug 修复）
#   - _flush_queue 先调用 apply_pending_marks
#   - "刷新状态"→"重启刷新"（重新注入 JS timer）
# ══════════════════════════════════════════════════════════════
class Dashboard:
    def __init__(self):
        self._uiq          = _UIQueue()
        self._state        = State()
        self._table        = PreviewTable()
        self._log          = LiveLog(self._uiq)
        self._status       = StatusBar(self._uiq)
        self._mode_cfg     = MODES['热门']
        self._mode_name    = '热门'
        self._stop_ev      = threading.Event()
        self._pause_ev     = threading.Event()
        self._w            = {}
        self._acc_mod      = None
        self._run_id       = 0
        self._cur_idx      = 0
        self._cur_total    = 0
        self._last_results = None

    # ── Colab 回调注册 ─────���──────────────────────────────────
    def _register_colab_flush(self):
        if not _IN_COLAB: return
        try:
            from google.colab import output as _co
            _co.register_callback('_yt_dl_flush', lambda: self._flush_queue())
        except Exception: pass

    # ── 异步线程刷新入口 ─────────────────────────────────────
    def _auto_flush(self):
        try:
            from IPython import get_ipython
            ip = get_ipython()
            if ip and hasattr(ip, 'kernel'):
                ip.kernel.io_loop.call_soon_threadsafe(self._flush_queue)
        except Exception: pass

    # ── 主 flush（主线程） ────────────────────────────────────
    def _flush_queue(self):
        # 1. 先应用行状态标记（下载进度着色）
        try: self._table.apply_pending_marks()
        except: pass

        # 2. 如有新搜索结果，渲染表格
        if self._last_results is not None:
            try:
                self._table.render(self._last_results)
                self._last_results = None
            except: pass

        # 3. 更新 log / status / 按钮
        pending, callbacks = self._uiq.drain()
        if 'log'    in pending: self._log._w.value    = pending['log']
        if 'status' in pending: self._status._w.value = pending['status']
        if 'dl_btn' in pending and 'dl' in self._w:
            self._w['dl'].description = pending['dl_btn']
        if 'prev_btn' in pending and pending['prev_btn'] == 'reset':
            if 'prev' in self._w:
                self._w['prev'].disabled    = False
                self._w['prev'].description = '搜索预览'
        for cb in callbacks:
            try: cb()
            except Exception as e:
                try: self._log.write(f'[flush_cb错误] {type(e).__name__}: {e}')
                except: pass

    # ── 构建 UI ───────────────────────────────────────────────
    def _build(self):
        S = {'description_width':'55px'}
        L = W.Layout
        mode_btns = []
        for mn, mc in MODES.items():
            b = W.Button(description=mn,
                         layout=L(width='76px', height='30px'),
                         style={'font_size':'12px','button_color':mc['color']},
                         tooltip=mc['desc'])
            def _om(_, _n=mn, _c=mc):
                self._flush_queue()
                self._mode_cfg = _c; self._mode_name = _n
                self._w['sort'].value  = _c['sort']
                self._w['count'].value = _c['count']
                for _b2, _n2 in zip(mode_btns, MODES):
                    _b2.style.button_color = (
                        '#37474f' if _n2 == _n else MODES[_n2]['color'])
                self._log.write(f'已切换【{_n}】{_c["desc"]}')
                self._flush_queue()
            b.on_click(_om); mode_btns.append(b)

        w_query = W.Text(placeholder='关键词 / YouTube URL',
                         description='搜索:', style=S, layout=L(width='98%'))
        w_sort  = W.Dropdown(options=list(SORT_OPTS.keys()), value='最多播放',
                             description='排序:', style=S, layout=L(width='155px'))
        w_count = W.IntSlider(value=15, min=1, max=50, step=1,
                              description='数量:', style=S,
                              layout=L(width='46%'), continuous_update=False)
        self._w['sort'] = w_sort; self._w['count'] = w_count

        # 固定模块（年份动态替换）
        mod_rows = []
        for cat, kws in KEYWORD_MODULES.items():
            btns = []
            for lbl, kw in kws.items():
                b2 = W.Button(description=lbl,
                              layout=L(width='auto', height='26px'),
                              style={'font_size':'11px'},
                              tooltip=f'搜索词: {kw}')
                def _ok(_b, _kw=kw):
                    yr = datetime.now().year
                    ky = _kw.replace('2026', str(yr)).replace('2025', str(yr-1))
                    w_query.value = ky
                    if self._acc_mod: self._acc_mod.selected_index = None
                b2.on_click(_ok); btns.append(b2)
            mod_rows.append(W.VBox([
                W.HTML(f'<div style="font-size:11px;font-weight:600;'
                       f'color:#555;padding:2px 0">{cat}</div>'),
                W.HBox(btns, layout=L(flex_flow='row wrap', margin='0 0 4px'))]))
        acc_mod = W.Accordion(
            children=[W.VBox(mod_rows, layout=L(padding='4px'))],
            layout=L(width='100%', margin='2px 0'))
        acc_mod.set_title(0, '固定模块（点击填入搜索词，悬停查看原词）')
        acc_mod.selected_index = None
        self._acc_mod = acc_mod

        # 设置面板
        w_cookie   = W.Text(value=Cfg.COOKIE, description='Cookie:',
                            style=S, layout=L(width='97%'))
        w_save     = W.Text(value=Cfg.SAVE_DIR, description='保存:',
                            style=S, layout=L(width='97%'))
        w_maxmb    = W.IntSlider(value=Cfg.MAX_MB, min=50, max=3000, step=50,
                                 description='大小上限:',
                                 style={'description_width':'65px'},
                                 layout=L(width='52%'), continuous_update=False)
        w_subtitle = W.Checkbox(value=False, description='下载字幕',
                                indent=False,
                                layout=L(width='auto'),
                                style={'description_width':'auto'})
        w_reset_btn = W.Button(description='重置下载记录',
                               button_style='warning', layout=L(width='115px'))
        w_reset_btn.on_click(lambda _: self._on_reset())

        acc_set = W.Accordion(
            children=[W.VBox([
                w_cookie, w_save,
                W.HBox([w_maxmb, W.HTML('&nbsp;&nbsp;'), w_subtitle],
                       layout=L(align_items='center')),
                w_reset_btn
            ], layout=L(padding='6px'))],
            layout=L(width='100%', margin='2px 0'))
        acc_set.set_title(0, '设置（Cookie / 路径 / 大小上限 / 字幕）')
        acc_set.selected_index = None
        self._w.update({'cookie':w_cookie, 'save':w_save,
                        'maxmb':w_maxmb, 'subtitle':w_subtitle})

        # 操作按钮
        w_prev   = W.Button(description='搜索预览', button_style='info',
                            layout=L(width='88px'))
        w_dl     = W.Button(description='开始下载', button_style='success',
                            layout=L(width='88px'))
        w_pause  = W.Button(description='⏸ 暂停', layout=L(width='88px'),
                            disabled=True,
                            style={'button_color':'#f57f17'})
        w_resume = W.Button(description='▶ 继续', button_style='success',
                            layout=L(width='76px'), disabled=True)
        w_stop   = W.Button(description='⏹ 终止', button_style='danger',
                            layout=L(width='76px'), disabled=True)
        w_refresh = W.Button(description='🔄 重启刷新',
                             layout=L(width='100px'),
                             style={'button_color':'#e8eaf6'},
                             tooltip='重新注入自动刷新定时器（2s间隔）')
        w_clear  = W.Button(description='清空预览', layout=L(width='80px'))
        w_clrl   = W.Button(description='清空日志', layout=L(width='80px'))
        self._w.update({'prev':w_prev, 'dl':w_dl, 'pause':w_pause,
                        'resume':w_resume, 'stop':w_stop})

        def _reset_btns():
            w_dl.disabled = False; w_dl.description = '开始下载'
            w_pause.disabled = True; w_pause.description = '⏸ 暂停'
            w_resume.disabled = True
            w_stop.disabled = True; w_stop.description = '⏹ 终止'
            w_prev.disabled = False

        def _params():
            Cfg.MAX_MB = w_maxmb.value
            return (w_query.value.strip(),
                    SORT_OPTS.get(w_sort.value, ''),
                    w_count.value,
                    w_cookie.value.strip(),
                    w_save.value.strip())

        def _on_refresh(_):
            self._flush_queue()
            if _IN_COLAB:
                try: display(HTML(_AUTO_REFRESH_JS))
                except: pass
            self._log.write('🔄 自动刷新已重启（2s 间隔）')
            self._flush_queue()

        w_refresh.on_click(_on_refresh)
        w_prev.on_click(lambda _: self._on_preview(*_params(), w_prev))
        w_dl.on_click(lambda _: self._on_download(
            *_params(), w_dl, w_pause, w_resume, w_stop, _reset_btns))
        w_pause.on_click(lambda _: self._on_pause(w_pause, w_resume))
        w_resume.on_click(lambda _: self._on_resume(w_pause, w_resume))
        w_stop.on_click(lambda _: self._on_stop())
        w_clear.on_click(lambda _: self._table.clear())
        w_clrl.on_click(lambda _: self._log.clear())

        btn_row = W.HBox(
            [w_prev, w_dl, w_pause, w_resume, w_stop,
             W.HTML('&nbsp;'), w_refresh, W.HTML('&nbsp;'), w_clear, w_clrl],
            layout=L(margin='6px 0', flex_flow='row wrap', align_items='center'))

        w_scroll = W.Box(
            [self._table.container],
            layout=L(width='100%', height='420px', overflow_y='scroll',
                     border='1px solid #ddd', border_radius='4px'))

        def _sep(t):
            return W.HTML(
                f'<div style="font-size:10px;color:#aaa;'
                f'border-bottom:1px solid #eee;'
                f'padding:2px 0;margin:6px 0 3px">{t}</div>')

        _log_acc = W.Accordion(children=[self._log.widget()],
                               layout=L(width='100%', margin='2px 0'))
        _log_acc.set_title(0, '下载日志（点此折叠 / 展开）')
        _log_acc.selected_index = 0

        _note = W.HTML(
            '<div style="font-size:11px;color:#888;background:#fffde7;'
            'border:1px solid #ffe082;border-radius:3px;'
            'padding:4px 10px;margin:3px 0">'
            '💡 状态每 2 秒自动刷新 · 暂停在<b>当前视频下完后</b>生效 · '
            '字幕开关在"设置"里 · 单击勾选 / 范围选择</div>')

        return W.VBox([
            W.HTML('<div style="font-size:15px;font-weight:600;'
                   'margin:4px 0 6px;color:#222;">'
                   'YouTube Downloader '
                   '<span style="font-size:11px;color:#aaa;font-weight:400">'
                   'v323</span></div>'),
            _sep('模式'),
            W.HBox(mode_btns, layout=L(margin='0 0 4px')),
            _sep('搜索'),
            w_query,
            W.HBox([w_sort, W.HTML('&nbsp;'), w_count]),
            acc_mod, acc_set, btn_row, _note,
            self._status.widget(),
            _sep('预览列表（范围选择 / 单击勾选）'),
            w_scroll, _log_acc,
        ], layout=W.Layout(border='1px solid #dde', padding='12px 14px', width='99%'))

    # ── 搜索预览 ──────────────────────────────────────────────
    def _on_preview(self, query, sort, count, cookie, save, w_prev):
        self._flush_queue()
        if not query:
            self._log.write('请输入关键词或URL')
            self._status.error('请输入关键词或URL')
            self._flush_queue(); return
        if query.startswith(('http://','https://')):
            self._log.write('检测到URL，点"开始下载"即可')
            self._flush_queue(); return
        url = _build_url(query, sort)
        if not url: return
        try: _check_cookie(cookie)
        except CookieError as e:
            self._log.write(f'Cookie错误:{e}')
            self._status.error(f'Cookie:{e}')
            self._flush_queue(); return
        w_prev.disabled = True; w_prev.description = '搜索中...'
        self._status.searching(query)
        self._log.write(f'正在搜索: {query}')
        self._flush_queue()

        def _search():
            try:
                res  = _do_search(url, count, cookie, self._mode_cfg)
                mode = self._mode_name
                self._last_results = res
                if res:
                    self._log.write(f'找到 {len(res)} 个（{mode}）')
                    self._status.found(len(res), mode)
                else:
                    self._log.write('未找到符合条件的视频')
                    self._status.error('未找到结果')
            except CookieError as e:
                self._log.write(f'Cookie失效:{e}')
                self._status.error(f'Cookie失效:{e}')
            except Exception as e:
                self._log.write(f'搜索出错:{type(e).__name__}:{e}')
                self._status.error(f'搜索出错:{type(e).__name__}')
            finally:
                self._uiq.put('prev_btn', 'reset')
                self._auto_flush()

        threading.Thread(target=_search, daemon=True).start()

    # ── 开始下载（核心 bug 修复） ──────────────────────────────
    def _on_download(self, query, sort, count, cookie, save,
                     w_dl, w_pause, w_resume, w_stop, reset_cb):
        self._flush_queue()
        subtitle_on  = self._w['subtitle'].value
        search_first = False

        # ★ v323 修复：正确区分三种状态
        if query.startswith(('http://','https://')):
            # 直接 URL
            items = [{'id':'', 'title':_trim(query, 52), 'url':query,
                      'channel':'', 'duration':'N/A', 'dur_s':0,
                      'view_count':None, 'upload_date':''}]
        else:
            selected = self._table.get_selected()
            if selected is None:
                # 从未预览 → 先搜索再下
                if not query:
                    self._log.write('请输入关键词或URL')
                    self._flush_queue(); return
                items = None
                search_first = True
            elif len(selected) == 0:
                # 有预览结果但全部取消勾选
                self._log.write('⚠ 请至少勾选一个视频再下载')
                self._status.error('请至少勾选一个视频')
                self._flush_queue(); return
            else:
                # 正常：使用勾选的视频
                items = sorted(selected,
                               key=lambda x: int(x.get('view_count') or 0),
                               reverse=True)

        w_dl.disabled = True; w_dl.description = '下载中...'
        w_pause.disabled = False; w_resume.disabled = True
        w_stop.disabled = False; w_stop.description = '⏹ 终止'
        self._run_id += 1; my_run_id = self._run_id
        self._stop_ev  = threading.Event()
        self._pause_ev = threading.Event()
        stop_ev  = self._stop_ev
        pause_ev = self._pause_ev

        def _guarded_reset():
            if self._run_id == my_run_id: reset_cb()

        def _prog(idx, n, _):
            self._cur_idx = idx; self._cur_total = n

        def _run():
            nonlocal items
            try:
                ok, msg = _mount_drive()
                self._log.write(f'Drive: {msg}')
                self._auto_flush()
                if not ok:
                    self._status.error(f'Drive连接失败:{msg}')
                    self._auto_flush(); return
                try:
                    found = _check_cookie(cookie)
                    self._log.write(f'Cookie OK: {found}')
                    self._auto_flush()
                except CookieError as e:
                    self._log.write(f'Cookie失效:{e}')
                    self._status.error(f'Cookie失效:{e}')
                    self._auto_flush(); return

                if search_first:
                    self._log.write('未预览，先搜索...')
                    surl = _build_url(query, sort)
                    if not surl:
                        self._log.write('无效输入'); return
                    try:
                        items = _do_search(surl, count, cookie, self._mode_cfg)
                        if not items:
                            self._log.write('未找到视频')
                            self._auto_flush(); return
                        self._last_results = items[:]
                        self._auto_flush()
                    except CookieError as e:
                        self._log.write(f'Cookie失效:{e}')
                        self._auto_flush(); return

                sd = _make_session_dir(save, self._mode_name, query, len(items))
                self._log.write(f'目录: {sd}')
                self._auto_flush()

                done, fails, sw, done_ids, tb, elapsed = _do_download(
                    items, cookie, save,
                    stop_ev, pause_ev,
                    self._state, sd,
                    self._log, self._status,
                    _prog,
                    flush_cb=self._auto_flush,
                    subtitle_on=subtitle_on,
                    table_mark_cb=self._table.mark)

                sl = next((k for k, v in SORT_OPTS.items() if v == sort), sort)
                _write_index(sd, self._mode_name, query, sl, items, done_ids)
                if sw == 'user_stop':
                    self._status.stopped(done, fails)
                else:
                    self._status.done(done, fails, tb, elapsed)

            except Exception:
                self._log.write('下载崩溃:')
                self._log.write(traceback.format_exc()[-600:])
                self._status.error('下载崩溃，见日志')
            finally:
                self._uiq.put_cb(_guarded_reset)
                self._auto_flush()

        threading.Thread(target=_run, daemon=True).start()

    # ── 暂停 / 继续 / 终止 ────────────────────────────────────
    def _on_pause(self, w_pause, w_resume):
        self._flush_queue()
        self._pause_ev.set()
        w_pause.disabled = True; w_resume.disabled = False
        self._status.paused(self._cur_idx, self._cur_total)
        self._log.write('⏸ 暂停请求已发送，当前视频下完后停止')
        self._flush_queue()

    def _on_resume(self, w_pause, w_resume):
        self._flush_queue()
        self._pause_ev.clear()
        w_pause.disabled = False; w_resume.disabled = True
        self._status.resuming()
        self._log.write('▶ 继续下载...')
        self._flush_queue()

    def _on_stop(self):
        self._flush_queue()
        self._stop_ev.set(); self._pause_ev.clear()
        if 'stop' in self._w:
            self._w['stop'].description = '停止中...'
            self._w['stop'].disabled    = True
        self._log.write('正在中断...')
        self._flush_queue()

    def _on_reset(self):
        self._state.reset(); self._table.clear()
        self._last_results = None
        self._log.write('下载记录已清除')
        self._status.idle(); self._flush_queue()

    # ── 启动 ──────────────────────────────────────────────────
    def launch(self):
        ok, msg = _mount_drive()
        display(self._build())
        self._register_colab_flush()
        if _IN_COLAB:
            try: display(HTML(_AUTO_REFRESH_JS))
            except: pass
        self._log.write(f'v323 就绪 | Drive:{msg}')
        self._log.write(f'FRAGS={Cfg.FRAGS} | chunk={Cfg.HTTP_CHUNK_MB}MB | 刷新=2s')
        self._log.write('暂停模式：视频级 | 字幕默认关 | 封面自动嵌入mp4')
        self._status.idle(); self._flush_queue()


# ══════════════════════════════════════════════════════════���═══
# ── 启动
# ══════════════════════════════════════════════════════════════
try:
    _INSTANCE._stop_ev.set()
    _INSTANCE._pause_ev.clear()
except Exception:
    pass

_INSTANCE = Dashboard()
_INSTANCE.launch()
print('✅ v323 就绪')