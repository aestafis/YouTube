# ══════════════════════════════════════════════════════════════
# BLOCK 0 ── 依赖
# ══════════════════════════════════════════════════════════════
import subprocess, sys, os, re, json, time, glob
import shutil, traceback, difflib, threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FT
from datetime import datetime
from html import escape
from urllib.parse import urlparse

def _pip(*pkgs):
    for p in pkgs:
        subprocess.run([sys.executable,'-m','pip','install','-q',
                        '--upgrade',p],
                       check=False, capture_output=True)

def _apt(*pkgs):
    apt = shutil.which('apt-get')
    if not apt:
        print('[WARN] apt-get unavailable; skipping apt installs')
        return False
    ok = True
    for p in pkgs:
        try:
            subprocess.run([apt,'install','-y','-q',p],
                           check=False, capture_output=True)
        except Exception:
            ok = False
    return ok

try:
    import yt_dlp
    try:    _ver = int(yt_dlp.version.__version__.split('.')[0])
    except: _ver = 0
    if _ver < 2025:
        _pip('yt-dlp')
        import importlib; importlib.reload(yt_dlp)
    print(f'[OK] yt-dlp {yt_dlp.version.__version__}')
except ImportError:
    _pip('yt-dlp'); import yt_dlp

try:
    import yt_dlp_ejs
    print('[OK] yt-dlp-ejs')
except ImportError:
    _pip('yt-dlp-ejs')
    try:
        import yt_dlp_ejs
        print('[OK] yt-dlp-ejs installed')
    except ImportError:
        print('[WARN] yt-dlp-ejs unavailable, continuing without it')

def _ensure_deno():
    def _try():
        try:
            return subprocess.run(
                ['deno','--version'], capture_output=True,
                text=True, timeout=8).returncode == 0
        except: return False
    if _try(): return
    for d in ['/root/.deno/bin', os.path.expanduser('~/.deno/bin')]:
        if os.path.isfile(os.path.join(d,'deno')):
            os.environ['PATH'] = d+':'+os.environ.get('PATH','')
            if _try(): return
    try:
        subprocess.run('curl -fsSL https://deno.land/install.sh|sh',
                       shell=True, timeout=120, capture_output=True)
        os.environ['PATH'] = '/root/.deno/bin:'+os.environ.get('PATH','')
    except: pass
_ensure_deno()

try:
    _ff_ok = subprocess.run(
        ['ffmpeg','-version'], capture_output=True, timeout=5
    ).returncode == 0
except Exception:
    _ff_ok = False
if not _ff_ok and not _apt('ffmpeg'):
    print('[WARN] ffmpeg unavailable; continuing')

try:
    import ipywidgets as W
    from IPython.display import display, HTML
except ImportError:
    _pip('ipywidgets')
    import ipywidgets as W
    from IPython.display import display, HTML

try:
    from google.colab import drive as _gdrive
    _IN_COLAB = True
except:
    _IN_COLAB = False

from yt_dlp.utils import DownloadError, MaxDownloadsReached
print('[OK] all deps ready')


# ══════════════════════════════════════════════════════════════
# BLOCK 1 ── 配置
# ══════════════════════════════════════════════════════════════
class Cfg:
    COOKIE_DIR = '/content/drive/MyDrive/YouTube_Cookies'
    COOKIE   = os.path.join(COOKIE_DIR,'youtube_cookies.txt')
    META_DIR = os.path.join(COOKIE_DIR,'metadata')
    SAVE_DIR = '/content/drive/MyDrive/YouTube_Downloads'
    TMP_DIR  = '/content/local_temp'
    STATE    = os.path.join(META_DIR,'yt_state.json')
    INDEX    = os.path.join(META_DIR,'yt_index.json')
    FRAGS    = 16
    DEDUP    = 0.82
    MAX_MB   = 0
    HTTP_CHUNK_MB         = 10
    SEARCH_SOCKET_TIMEOUT = 10
    SEARCH_HARD_TIMEOUT   = 40
    SEARCH_FALLBACK_RATIO = 0.6
    SUB_SPLIT_TOKENS      = 1800

    @staticmethod
    def fix(p):
        p = os.path.expanduser(p)
        if p.startswith('/root/drive/'):
            p = '/content/drive/'+p[len('/root/drive/'):]
        return p

# A-4: 只保留两个排序
SORT_OPTS = {'relevance': '', 'views': 'viewcount'}
_SP       = {'viewcount': '&sp=CAM%3D', '': ''}

SORT_LABELS = {'relevance': '相关性', 'views': '最多播放'}
MODE_LABELS = {'Hot': '热门', 'Quality': '深度', 'Short': '短视频'}

MODES = {
    'Hot':     {'desc':'热门视频，按播放量排序',
                'sort':'views','min_dur':0,'max_dur':None,
                'neg_kw':True,'count':15,'color':'#e65100'},
    'Quality': {'desc':'深度长视频（10分钟以上）',
                'sort':'relevance','min_dur':600,'max_dur':None,
                'neg_kw':True,'count':10,'color':'#2e7d32'},
    'Short':   {'desc':'5分钟以内精品短内容',
                'sort':'views','min_dur':0,'max_dur':300,
                'neg_kw':False,'count':20,'color':'#1565c0'},
}

NEG_KW = re.compile(
    r'\b(reaction|reacting|unboxing|shocking|giveaway|prank|'
    r'make money|earn \$|passive income|clickbait|subscribe now)\b',re.I)
AD_KW  = re.compile(
    r'\b(sponsored|advertisement|\bad\b|promo code|'
    r'use code|affiliate|discount code)\b',re.I)

_CHANNEL_RE = re.compile(
    r'youtube\.com/(@[^/?#\s]+|channel/[^/?#\s]+|c/[^/?#\s]+'
    r'|user/[^/?#\s]+|playlist\?list=)',re.I)
# YouTube 视频 ID：固定 11 位，仅由 base64url 字符集 [A-Za-z0-9_-] 组成。
_YT_VIDEO_ID_RE = re.compile(r'^[A-Za-z0-9_-]{11}$')

# 固定模块（中文标签 + 英文搜索词）
KEYWORD_MODULES = {
    'AI / 机器学习': {
        'Transformer 原理': ('transformer explained from scratch',   'AI原理'),
        'LLM 实战':         ('LLM implementation tutorial 2026',     '大模型实战'),
        'AI Agent':         ('AI agent build from scratch',          '智能体开发'),
        '论文精读':         ('AI research paper explained 2026',     '论文精读'),
        '开源项目':         ('open source AI tools github 2026',     '开源项目'),
    },
    '编程': {
        'Python 实战':   ('python project tutorial 2026',         'Python实战'),
        '算法讲解':       ('algorithm explained visually',         '算法讲解'),
        '系统设计':       ('system design explained',              '系统设计'),
        '数学直觉':       ('math intuition visual explanation',    '数学直觉'),
        '前端开发':       ('web development tutorial 2026',        '前端开发'),
    },
    '游戏': {
        '精彩时刻':      ('game highlights 2026',                 '精彩时刻'),
        '独立游戏':      ('indie game review 2026',               '独立游戏'),
        '速通纪录':      ('speedrun world record 2026',           '速通纪录'),
        '游戏设计':      ('game design analysis deep dive',       '游戏设计'),
    },
    '动漫': {
        '深度分析':      ('anime video essay 2025',               '深度分析'),
        '作画赏析':      ('anime sakuga breakdown',               '作画赏析'),
        '新番推荐':      ('best anime 2025 recommendation',       '新番推荐'),
        '漫改对比':      ('manga vs anime adaptation comparison', '漫改对比'),
    },
    '娱乐': {
        '热门电影':      ('free movies 2026 full length',            '热门电影'),
        '电影预告':      ('movie trailer 2026',                   '电影预告'),
        '深度纪录片':    ('documentary full length 2026',         '深度纪录片'),
        '喜剧短片':      ('comedy sketch 2026',                   '喜剧短片'),
        '近期爆款':      ('most viral video 2026',                '近期爆款'),
    },
}


# ══════════════════════════════════════════════════════════════
# BLOCK 2 ── VideoIndex（延迟加载）
# ══════════════════════════════════════════════════════════════
class VideoIndex:
    def __init__(self):
        self._path  = None
        self._lock  = threading.Lock()
        self._cache = None

    def _ensure_path(self):
        if self._path is None:
            self._path = Cfg.fix(Cfg.INDEX)

    def _read_raw(self):
        self._ensure_path()
        try:
            if not os.path.exists(self._path):
                return {'updated':'','videos':{}}
            with open(self._path,encoding='utf-8') as f:
                d = json.load(f)
            if isinstance(d.get('videos'),dict): return d
        except Exception: pass
        return {'updated':'','videos':{}}

    def load(self):
        with self._lock:
            if self._cache is None:
                self._cache = self._read_raw()['videos']
            return dict(self._cache)

    def invalidate(self):
        with self._lock: self._cache = None

    def write(self, vid, title, channel, session=''):
        if not vid: return
        with self._lock:
            try:
                self._ensure_path()
                os.makedirs(os.path.dirname(self._path),exist_ok=True)
                raw = self._read_raw()
                raw['videos'][vid] = {
                    'title':title,'channel':channel,
                    'saved_at':datetime.now().isoformat(),
                    'session':session}
                raw['updated'] = datetime.now().isoformat()
                tmp = self._path+'.tmp'
                with open(tmp,'w',encoding='utf-8') as f:
                    json.dump(raw,f,ensure_ascii=False,indent=2)
                os.replace(tmp,self._path)
                if self._cache is not None:
                    self._cache[vid] = raw['videos'][vid]
            except Exception: pass

    def get_all_ids(self): return set(self.load().keys())

    def replace_all(self, videos_map):
        with self._lock:
            try:
                self._ensure_path()
                os.makedirs(os.path.dirname(self._path), exist_ok=True)
                safe_map = (videos_map if isinstance(videos_map, dict)
                            else {})
                raw = {'updated': datetime.now().isoformat(),
                       'videos': safe_map}
                tmp = self._path + '.tmp'
                with open(tmp, 'w', encoding='utf-8') as f:
                    json.dump(raw, f, ensure_ascii=False, indent=2)
                os.replace(tmp, self._path)
                self._cache = dict(safe_map)
            except Exception:
                pass


# ══════════════════════════════════════════════════════════════
# BLOCK 3 ── State（延迟加载，B-1修复：只有成功才标记loaded）
# ══════════════════════════════════════════════════════════════
class State:
    def __init__(self, index: VideoIndex):
        self._p      = None
        self._dl     = set()
        self._fail   = {}
        self._index  = index
        self._loaded = False
        self._lock   = threading.Lock()

    def _ensure_loaded(self):
        with self._lock:
            if self._loaded: return
            try:
                self._p = Cfg.fix(Cfg.STATE)
                if not os.path.exists(self._p):
                    self._loaded = True   # 文件不存在是正常情况
                    return
                with open(self._p,encoding='utf-8') as f:
                    d = json.load(f)
                self._dl   = set(d.get('downloaded',[]))
                self._fail = d.get('failed',{})
                self._loaded = True       # 只有成功才标记
            except Exception:
                pass                      # 失败保持False，下次重试

    def _save(self):
        try:
            if self._p is None: self._p = Cfg.fix(Cfg.STATE)
            os.makedirs(os.path.dirname(self._p),exist_ok=True)
            tmp = self._p+'.tmp'
            with open(tmp,'w',encoding='utf-8') as f:
                json.dump({'downloaded':list(self._dl),
                           'failed':self._fail,
                           'updated':datetime.now().isoformat()},
                          f,ensure_ascii=False,indent=2)
            os.replace(tmp,self._p)
        except Exception: pass

    def done(self,v,title='',channel='',session=''):
        self._ensure_loaded()
        self._dl.add(v); self._fail.pop(v,None); self._save()
        self._index.write(v,title,channel,session)

    def fail(self,v,t,r):
        self._ensure_loaded()
        x = self._fail.setdefault(v,{'title':t,'count':0})
        x['reason']=r; x['count']+=1

    def is_done(self,v):
        self._ensure_loaded(); return v in self._dl

    def can_retry(self,v):
        self._ensure_loaded()
        return self._fail.get(v,{}).get('count',0) < 3

    def get_failed_map(self):
        self._ensure_loaded()
        return dict(self._fail)

    def clear_failed(self, ids=None):
        self._ensure_loaded()
        if ids is None:
            self._fail.clear()
        else:
            for vid in set(ids):
                self._fail.pop(vid, None)
        self._save()

    def get_dl_set(self):
        self._ensure_loaded(); return set(self._dl)

    def replace_downloaded_ids(self, ids):
        self._ensure_loaded()
        self._dl = set(i for i in (ids or set()) if _is_valid_video_id(i))
        self._fail = {k: v for k, v in self._fail.items() if k in self._dl}
        self._save()

    def reset(self,clear_index=False):
        self._ensure_loaded()
        self._dl.clear(); self._fail.clear(); self._save()
        if clear_index:
            try:
                p = Cfg.fix(Cfg.INDEX)
                if os.path.exists(p): os.remove(p)
            except Exception: pass
            self._index.invalidate()


# ══════════════════════════════════════════════════════════════
# BLOCK 4 ── 工具函数（A-3: 删除_fmt_age）
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

def _trim(t, mx=36):
    if not t: return ''
    w = 0
    for i,c in enumerate(t):
        w += 2 if ord(c) > 0x2E7F else 1
        if w > mx: return t[:i]+'...'
    return t

def _safe_name_token(s, mx=24, fallback='unknown'):
    s = str(s or '').strip()
    s = re.sub(r'[\\/:*?"<>|]','_',s)
    s = re.sub(r'\s+','_',s)
    s = re.sub(r'_+','_',s).strip('_')
    if not s: s = fallback
    return s[:mx]

def _source_label_from_query(query):
    itype,idata=_parse_input(query or '')
    if itype=='keyword':
        return _safe_name_token(idata,mx=18,fallback='关键词')
    if itype=='single_url':
        return '单URL'
    if itype=='multi_url':
        return '多URL'
    if itype=='channel':
        return '频道URL'
    if itype=='channel_multi_warn':
        return '多频道URL'
    return 'URL'

def _dedup(title, seen, thr=0.82):
    nt = re.sub(r'[^\w\u4e00-\u9fff]','',(title or '').lower())
    if not nt: return False
    for et in seen[-30:]:
        ne = re.sub(r'[^\w\u4e00-\u9fff]','',et.lower())
        if difflib.SequenceMatcher(None,nt,ne).ratio() >= thr: return True
    return False

def _make_session_dir(base, mode, query, count):
    now=datetime.now()
    dt =f'{now.year%100}年{now.month}月{now.day}日_{now.hour:02d}点{now.minute:02d}分'
    direction=_safe_name_token(MODE_LABELS.get(mode,mode),mx=10,fallback='方向')
    source=_source_label_from_query(query)
    p=os.path.join(
        Cfg.fix(base),
        f'{dt}_{direction}_{source}_{int(count)}条')
    os.makedirs(p,exist_ok=True); return p

def _embed_thumb(video_path, thumb_path):
    if not os.path.exists(thumb_path): return False
    if os.path.splitext(video_path)[1].lower() not in ('.mp4','.m4v'):
        return False
    tmp = video_path+'._emb_.mp4'
    try:
        r = subprocess.run(
            ['ffmpeg','-loglevel','error','-y',
             '-i',video_path,'-i',thumb_path,
             '-map','0','-map','1','-c','copy',
             '-disposition:v:1','attached_pic',tmp],
            capture_output=True,timeout=90)
        if r.returncode==0 and os.path.exists(tmp) and \
                os.path.getsize(tmp)>1024:
            os.replace(tmp,video_path)
            try: os.remove(thumb_path)
            except: pass
            return True
        if os.path.exists(tmp):
            try: os.remove(tmp)
            except: pass
        return False
    except Exception:
        if os.path.exists(tmp):
            try: os.remove(tmp)
            except: pass
        return False

def _rename_with_index(session_dir, vid_order):
    for idx,(vid,title) in enumerate(vid_order,1):
        if not vid: continue
        safe   = re.sub(r'[\\/:*?"<>|]','_',title)[:40]
        prefix = f'{idx:02d}_'
        id_pfx = vid+'__'
        try:
            for fn in list(os.listdir(session_dir)):
                if fn.startswith('README'): continue
                if '._emb_' in fn: continue
                if fn.startswith(id_pfx) and not fn.startswith(prefix):
                    src  = os.path.join(session_dir,fn)
                    ext  = os.path.splitext(fn[len(id_pfx):])[1]
                    dst  = os.path.join(session_dir,f'{prefix}{safe}{ext}')
                    try: os.rename(src,dst)
                    except: pass
        except: pass

def _package_by_video(session_dir, vid_order, log=None):
    for idx,(vid,title) in enumerate(vid_order,1):
        if not vid: continue
        folder=f'{idx:02d}_{_safe_name_token(title,mx=40,fallback=vid)}'
        folder_path=os.path.join(session_dir,folder)
        try: os.makedirs(folder_path,exist_ok=True)
        except Exception: continue
        id_pfx=vid+'__'
        for fn in list(os.listdir(session_dir)):
            src=os.path.join(session_dir,fn)
            if not os.path.isfile(src): continue
            if not fn.startswith(id_pfx): continue
            base_name=fn[len(id_pfx):] or fn
            dst_name=base_name
            dst=os.path.join(folder_path,dst_name)
            i=1
            while os.path.exists(dst):
                stem,ext=os.path.splitext(base_name)
                dst_name=f'{stem}_{i}{ext}'
                dst=os.path.join(folder_path,dst_name)
                i+=1
            try:
                os.replace(src,dst)
                if log: log.write(f'pack: {folder}/{dst_name}')
            except Exception:
                pass

def _estimate_tokens(text):
    s=str(text or '')
    if not s: return 0
    cjk=len(re.findall(r'[\u3400-\u9fff]',s))
    words=len(re.findall(r"[A-Za-z0-9_']+",s))
    return max(1,cjk + int(words*1.3))

def _split_subtitle_text(raw):
    blocks=[]; cur_ts=''; cur=[]
    lines=(raw or '').splitlines()
    for line in lines:
        s=line.strip()
        if not s:
            if cur:
                txt=' '.join(x.strip() for x in cur if x.strip())
                if txt:
                    blocks.append((cur_ts,txt))
                cur=[]; cur_ts=''
            continue
        if s.upper().startswith('WEBVTT'):
            continue
        if re.fullmatch(r'\d+',s):
            continue
        if '-->' in s:
            cur_ts=s
            continue
        if s.startswith('NOTE ') or s in ('NOTE','STYLE','REGION'):
            continue
        s=re.sub(r'<[^>]+>','',s)
        if s:
            cur.append(s)
    if cur:
        txt=' '.join(x.strip() for x in cur if x.strip())
        if txt:
            blocks.append((cur_ts,txt))
    return blocks

def _split_subtitle_files(session_dir, vid, idx, title,
                          max_tokens=1200, log=None):
    if not vid: return []
    id_pfx=vid+'__'
    src_files=[]
    try:
        for fn in sorted(os.listdir(session_dir)):
            if not fn.startswith(id_pfx): continue
            if '._emb_' in fn: continue
            ext=os.path.splitext(fn)[1].lower()
            if ext not in {'.vtt','.srt','.ass','.ssa','.lrc'}:
                continue
            src_files.append(fn)
    except Exception:
        return []
    if not src_files: return []

    out_dir=os.path.join(
        session_dir,'subtitle_chunks',
        f'{idx:02d}_{_safe_name_token(title,mx=40,fallback=vid)}')
    try: os.makedirs(out_dir,exist_ok=True)
    except Exception: return []

    all_parts=[]; part_no=1
    for fn in src_files:
        p=os.path.join(session_dir,fn)
        try:
            with open(p,'r',encoding='utf-8',errors='ignore') as f:
                raw=f.read()
        except Exception:
            continue
        blocks=_split_subtitle_text(raw)
        if not blocks:
            txt=re.sub(r'\s+',' ',raw).strip()
            if txt: blocks=[('',txt)]
        if not blocks: continue

        lang='mixed'
        m=re.search(r'\.([A-Za-z-]{2,16})\.(?:vtt|srt|ass|ssa|lrc)$',fn,re.I)
        if m: lang=m.group(1)
        chunk=[]; cur_tk=0
        for ts,txt in blocks:
            line=(f'[{ts}] {txt}' if ts else txt).strip()
            tk=_estimate_tokens(line)
            if chunk and (cur_tk+tk)>max_tokens:
                dst_name=f'{vid}_{lang}_part{part_no:03d}.txt'
                dst=os.path.join(out_dir,dst_name)
                with open(dst,'w',encoding='utf-8') as f:
                    f.write('\n'.join(chunk)+'\n')
                all_parts.append({
                    'video_id':vid,'video_order':idx,'video_title':title,
                    'source_file':fn,'part':part_no,'token_estimate':cur_tk,
                    'line_count':len(chunk),
                    'path':os.path.relpath(dst,session_dir),
                })
                part_no+=1; chunk=[]; cur_tk=0
            chunk.append(line); cur_tk+=tk
        if chunk:
            dst_name=f'{vid}_{lang}_part{part_no:03d}.txt'
            dst=os.path.join(out_dir,dst_name)
            with open(dst,'w',encoding='utf-8') as f:
                f.write('\n'.join(chunk)+'\n')
            all_parts.append({
                'video_id':vid,'video_order':idx,'video_title':title,
                'source_file':fn,'part':part_no,'token_estimate':cur_tk,
                'line_count':len(chunk),
                'path':os.path.relpath(dst,session_dir),
            })
            part_no+=1

    if all_parts and log:
        log.write(f'subtitle split: {idx:02d} {title[:36]} -> {len(all_parts)} parts')
    return all_parts

def _write_index_txt(sd, mode, query, sort_lbl, items, done_ids):
    lines = ['=== YouTube Download Index ===',
             f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
             f'Query: {query}  Mode: {mode}  Sort: {sort_lbl}',
             f'Planned: {len(items)}  Done: {len(done_ids)}','',
             '--- List ---']
    for i,x in enumerate(items,1):
        ok = 'OK' if x.get('id','') in done_ids else '--'
        v  = _fmt_views(x.get('view_count')) or '-'
        lines += [f'[{ok}] {i:02d}. [{v}] {x.get("title","")[:55]}',
                  f'      {x.get("channel","N/A")}',
                  f'      {x.get("url","")}','']
    now=datetime.now()
    dt=f'{now.year%100}年{now.month}月{now.day}日_{now.hour:02d}点{now.minute:02d}分'
    mode_lbl=_safe_name_token(MODE_LABELS.get(mode,mode),mx=10,fallback='方向')
    src_lbl=_source_label_from_query(query)
    p = os.path.join(
        sd,
        f'索引_{mode_lbl}_{src_lbl}_{len(done_ids)}条_{dt}.txt')
    with open(p,'w',encoding='utf-8') as f: f.write('\n'.join(lines))
    return p

def _write_index_json(sd, mode, query, sort_lbl, items, done_ids,
                      subtitle_parts=None):
    data={
        'generated_at':datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'mode':mode,
        'query':query,
        'sort':sort_lbl,
        'planned':len(items or []),
        'done':len(done_ids or set()),
        'done_ids':sorted(done_ids or set()),
        'videos':[],
        'subtitle_parts':subtitle_parts or [],
    }
    done_set=set(done_ids or set())
    for i,x in enumerate(items or [],1):
        data['videos'].append({
            'order':i,
            'id':x.get('id',''),
            'done':x.get('id','') in done_set,
            'title':x.get('title',''),
            'channel':x.get('channel',''),
            'url':x.get('url',''),
            'view_count':x.get('view_count'),
            'duration':x.get('duration'),
        })
    p=os.path.join(sd,'索引_下载结果.json')
    with open(p,'w',encoding='utf-8') as f:
        json.dump(data,f,ensure_ascii=False,indent=2)
    return p

def _extract_urls(raw):
    urls = re.findall(r'https?://(?:(?!https?://).)+',raw)
    cleaned = []
    for u in urls:
        u = re.sub(r'[)\]}>.,;:!?\'"]+$','',u).strip()
        if u: cleaned.append(u)
    return cleaned

def _parse_input(raw):
    raw = raw.strip()
    if not raw: return 'keyword',''
    urls = _extract_urls(raw)
    if not urls: return 'keyword',raw
    if len(urls)==1:
        u = urls[0]
        if _CHANNEL_RE.search(u): return 'channel',u
        return 'single_url',u
    channels = [u for u in urls if _CHANNEL_RE.search(u)]
    if channels:
        if len(channels)>1: return 'channel_multi_warn',channels
        return 'channel',channels[0]
    return 'multi_url',urls

_SUB_EXTS = frozenset({'.vtt', '.srt', '.ass', '.ssa', '.lrc', '.txt'})


def _session_has_video_artifacts(session_dir, order, title, vid):
    if not os.path.isdir(session_dir):
        return False

    pref = f'{int(order):02d}_'
    exts = _VIDEO_EXTS
    try:
        for fn in os.listdir(session_dir):
            p = os.path.join(session_dir, fn)
            if os.path.isfile(p):
                if fn.startswith(pref) and os.path.splitext(fn)[1].lower() in exts:
                    return True
    except Exception:
        pass

    folder = os.path.join(
        session_dir,
        f'{int(order):02d}_{_safe_name_token(title, mx=40, fallback=vid)}')
    if not os.path.isdir(folder):
        return False
    try:
        for fn in os.listdir(folder):
            p = os.path.join(folder, fn)
            if os.path.isfile(p):
                if os.path.splitext(fn)[1].lower() in exts:
                    return True
    except Exception:
        pass
    return False


def _load_index_raw(path):
    p = Cfg.fix(path)
    try:
        if os.path.exists(p):
            with open(p, encoding='utf-8') as f:
                d = json.load(f)
            if isinstance(d, dict) and isinstance(d.get('videos'), dict):
                return d
    except Exception:
        pass
    return {'updated': '', 'videos': {}}


def _rebuild_index_from_sessions(save_dir, index_path):
    save_dir = Cfg.fix(save_dir)
    old_raw = _load_index_raw(index_path)
    old_videos = old_raw.get('videos', {}) or {}
    rebuilt = {}
    scanned_sessions = 0
    scanned_videos = 0

    for root, _dirs, files in os.walk(save_dir):
        if '索引_下载结果.json' not in files:
            continue
        scanned_sessions += 1
        jp = os.path.join(root, '索引_下载结果.json')
        try:
            with open(jp, encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            continue
        videos = data.get('videos', []) or []
        for v in videos:
            scanned_videos += 1
            vid = (v.get('id') or '').strip()
            if (not _is_valid_video_id(vid)) or (not v.get('done')):
                continue
            order = int(v.get('order') or 0)
            title = str(v.get('title') or '').strip()
            if order <= 0:
                continue
            if not _session_has_video_artifacts(root, order, title, vid):
                continue
            old = old_videos.get(vid, {}) if isinstance(old_videos, dict) else {}
            rebuilt[vid] = {
                'title': old.get('title') or title,
                'channel': old.get('channel') or str(v.get('channel') or ''),
                'saved_at': old.get('saved_at') or datetime.now().isoformat(),
                'session': old.get('session') or os.path.basename(root),
            }

    return rebuilt, {
        'sessions': scanned_sessions,
        'videos': scanned_videos,
        'kept': len(rebuilt),
        'old': len(old_videos),
        'dropped': max(0, len(old_videos) - len(rebuilt)),
    }


# ══════════════════════════════════════════════════════════════
# BLOCK 5 ── UIQueue + StatusBar（A-8: 深色主题配色）
# ══════════════════════════════════════════════════════════════
class _UIQueue:
    def __init__(self):
        self._lock      = threading.Lock()
        self._pending   = {}
        self._callbacks = []

    def put(self,kind,value):
        with self._lock: self._pending[kind]=value

    def put_cb(self,fn):
        with self._lock: self._callbacks.append(fn)

    def drain(self):
        with self._lock:
            p=dict(self._pending); c=list(self._callbacks)
            self._pending.clear(); self._callbacks.clear()
        return p,c

# A-8: 深色主题友好配色，左边框而非背景色
_SB_TMPL = (
    '<div style="font-size:13px;font-family:monospace;'
    'background:{bg};color:{fg};padding:7px 14px;'
    'border-radius:4px;border-left:3px solid {border};'
    'line-height:1.8;margin:2px 0;'
    'white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'
    '<b>[{icon}]</b>&nbsp;&nbsp;{msg}</div>')

def _sb(style, icon, msg):
    styles = {
        'idle':  ('rgba(255,255,255,0.04)','#888','#555'),
        'info':  ('rgba(21,101,192,0.18)','#90caf9','#1565c0'),
        'ok':    ('rgba(46,125,50,0.18)','#a5d6a7','#2e7d32'),
        'dl':    ('rgba(230,81,0,0.18)','#ffb74d','#e65100'),
        'pause': ('rgba(198,40,40,0.18)','#ef9a9a','#c62828'),
        'stop':  ('rgba(255,255,255,0.04)','#888','#555'),
        'err':   ('rgba(198,40,40,0.18)','#ef9a9a','#c62828'),
    }
    bg,fg,border = styles.get(style,styles['idle'])
    return _SB_TMPL.format(bg=bg,fg=fg,border=border,icon=icon,msg=msg)


class StatusBar:
    def __init__(self,uiq):
        self._uiq=uiq
        self._w=W.HTML(
            value=_sb('idle','  ','就绪'),
            layout=W.Layout(width='100%'))

    def _push(self,html): self._uiq.put('status',html)

    def idle(self):
        self._push(_sb('idle','  ','就绪'))
    def searching(self,q):
        self._push(_sb('info','S',f'搜索中: {_trim(q,48)}'))
    def fetching_urls(self,n):
        self._push(_sb('info','S',f'正在读取 {n} 条视频...'))
    def fetching_channel(self,name):
        self._push(_sb('info','S',f'读取频道: {_trim(name,44)}'))
    def found(self,n,mode,skipped=0):
        sk=f'（跳过 {skipped} 条已下载）' if skipped else ''
        self._push(_sb('ok','OK',
            f'找到 {n} 条（{mode}）{sk} — 勾选后点下载'))
    def downloading(self,idx,n,title):
        self._push(_sb('dl','>',
            f'下载中 {idx}/{n} | {_trim(title,44)}'))
    def update_progress(self,pct,idx,n,title):
        self._push(_sb('dl','>',
            f'{int(pct)}%  {idx}/{n} | {_trim(title,44)}'))
    def paused(self,idx,n):
        self._push(_sb('pause','||',
            f'当前视频结束后暂停（{idx}/{n}）— 点击继续'))
    def paused_after(self,done,n):
        self._push(_sb('pause','||',
            f'已暂停（完成 {done}/{n}）— 点击继续'))
    def resuming(self):
        self._push(_sb('ok','>','继续下载中...'))
    def done(self,done,fails,size,elapsed):
        self._push(_sb('ok','+',
            f'完成 +{done} -{fails}  {_fmt_size(size)}  {elapsed:.0f}s'))
    def stopped(self,done,fails):
        self._push(_sb('stop','[]',f'已停止  +{done} -{fails}'))
    def cancelled(self):
        self._push(_sb('stop','x','搜索已取消'))
    def error(self,msg):
        self._push(_sb('err','!',msg))
    def widget(self): return self._w


# ══════════════════════════════════════════════════════════════
# BLOCK 6 ── LiveLog
# ══════════════════════════════════════════════════════════════
class LiveLog:
    _MAX = 200
    def __init__(self,uiq):
        self._uiq  = uiq
        self._w    = W.Textarea(
            value='',disabled=True,
            placeholder='Download log...',
            layout=W.Layout(width='100%',height='220px'))
        self._lock = threading.Lock()
        self._lines= []

    def write(self,msg):
        ts = datetime.now().strftime('%H:%M:%S')
        line = f'[{ts}] {msg}'
        with self._lock:
            self._lines.append(line)
            if len(self._lines)>self._MAX:
                self._lines=self._lines[-self._MAX:]
            text='\n'.join(self._lines)
        self._uiq.put('log',text)

    def clear(self):
        with self._lock: self._lines=[]
        self._uiq.put('log','')

    def widget(self): return self._w


# ══════════════════════════════════════════════════════════════
# BLOCK 7 ── Cookie + Drive
# ══════════════════════════════════════════════════════════════
class CookieError(Exception): pass

def _mount_drive():
    if os.path.ismount('/content/drive'): return True,'already mounted'
    if not _IN_COLAB: return False,'not Colab'
    try:
        _gdrive.mount('/content/drive',force_remount=False)
        return os.path.ismount('/content/drive'),'mounted'
    except Exception as e: return False,str(e)

def _colab_preflight():
    if _IN_COLAB:
        ok,_=_mount_drive()
        if not ok: return
    for d in (Cfg.COOKIE_DIR,Cfg.META_DIR,Cfg.SAVE_DIR,Cfg.TMP_DIR):
        try: os.makedirs(Cfg.fix(d),exist_ok=True)
        except Exception: pass

def _looks_like_dir_path(path):
    p=(path or '').strip()
    if not p: return False
    if p.endswith('/'): return True
    base=os.path.basename(p)
    return ('.' not in base)

def _cookie_json_to_netscape_txt(cookies):
    lines=['# Netscape HTTP Cookie File',
           '# Generated from JSON cookie export']
    for c in (cookies or []):
        if not isinstance(c,dict): continue
        domain=str(c.get('domain','') or '').strip()
        name  =str(c.get('name','') or '').strip()
        value =str(c.get('value','') or '').replace('\t',' ').replace('\r','').replace('\n','')
        if not domain or not name: continue
        path  =str(c.get('path','/') or '/')
        secure='TRUE' if bool(c.get('secure')) else 'FALSE'
        include_sub='TRUE' if (domain.startswith('.')
                               or not bool(c.get('hostOnly',False))) else 'FALSE'
        exp=c.get('expirationDate',0)
        try: exp_i=max(0,int(float(exp)))
        except Exception: exp_i=0
        lines.append('\t'.join([
            domain,include_sub,path,secure,str(exp_i),name,value
        ]))
    if len(lines)<=2:
        raise CookieError('JSON 中没有可用的 Cookie 条目')
    return '\n'.join(lines)+'\n'

def _normalize_cookie_file(path):
    p=Cfg.fix(path)
    if not os.path.isfile(p): return p
    try:
        with open(p,encoding='utf-8',errors='ignore') as f:
            head=f.read(4096).lstrip()
    except Exception:
        return p

    # Also support JSON pasted into a .txt cookie file.
    is_json = p.lower().endswith('.json') or (
        head[:1] in ('[','{') and '"domain"' in head and '"name"' in head
    )
    if not is_json: return p

    try:
        with open(p,encoding='utf-8',errors='ignore') as f:
            raw=json.load(f)
        if isinstance(raw,dict) and isinstance(raw.get('cookies'),list):
            cookies=raw['cookies']
        elif isinstance(raw,list):
            cookies=raw
        else:
            raise CookieError('JSON Cookie 格式不支持')

        base,_=os.path.splitext(p)
        txt_out=f'{base}_auto_netscape.txt'
        need_write=True
        try:
            need_write=(os.path.getmtime(txt_out) < os.path.getmtime(p))
        except OSError:
            need_write=True
        if need_write:
            txt=_cookie_json_to_netscape_txt(cookies)
            with open(txt_out,'w',encoding='utf-8') as f:
                f.write(txt)
        return txt_out
    except CookieError:
        raise
    except Exception as e:
        raise CookieError(f'Cookie JSON 解析失败: {type(e).__name__}')

def _resolve_cookie_file(path, create_dir=False):
    p=Cfg.fix((path or '').strip())
    if not p: raise CookieError('Cookie 路径为空')

    use_dir = os.path.isdir(p) or _looks_like_dir_path(p)
    if use_dir:
        cdir=p.rstrip('/')
        cdir_real=os.path.realpath(cdir)
        if create_dir:
            os.makedirs(cdir,exist_ok=True)
        elif not os.path.isdir(cdir):
            raise CookieError(f'目录不存在:{cdir}')

        pats=('youtube_cookies*.txt','youtube_cookies*.json',
              '*cookie*.txt','*cookie*.json','*.txt','*.json')
        files=[]; seen=set()
        for pat in pats:
            for fp in glob.glob(os.path.join(cdir,pat)):
                if not os.path.isfile(fp): continue
                if os.path.islink(fp): continue
                if fp.endswith('_auto_netscape.txt'): continue
                real_fp=os.path.realpath(fp)
                try:
                    if os.path.commonpath([cdir_real,real_fp])!=cdir_real:
                        continue
                except ValueError:
                    continue
                if fp not in seen:
                    files.append(fp); seen.add(fp)
        def _mtime(fp):
            try: return os.path.getmtime(fp)
            except OSError: return -1
        files.sort(key=_mtime, reverse=True)
        if not files:
            raise CookieError(
                f'未找到 Cookie 文件，请在目录中放入 *.txt: {cdir}')
        return _normalize_cookie_file(files[0])

    parent=os.path.dirname(p)
    if create_dir and parent:
        os.makedirs(parent,exist_ok=True)
    return _normalize_cookie_file(p)

def _check_cookie(path):
    path = Cfg.fix(path)
    if not os.path.exists(path): raise CookieError(f'not found:{path}')
    if os.path.getsize(path)==0:  raise CookieError('empty file')
    # B-5: 读取上限改为 512KB
    with open(path,encoding='utf-8',errors='ignore') as f:
        c = f.read(512*1024)
    if 'youtube.com' not in c: raise CookieError('no youtube.com entry')
    found=[k for k in ('SAPISID','__Secure-1PSID','LOGIN_INFO') if k in c]
    if not found: raise CookieError('no login info found')
    return found


# ══════════════════════════════════════════════════════════════
# BLOCK 8 ── 搜索 / 信息抓取
# ══════════════════════════════════════════════════════════════
def _build_url(query, sort_key):
    q = query.strip()
    if not q: return None
    if q.startswith(('http://','https://')): return q
    sp = _SP.get(sort_key,'')
    return (f'https://www.youtube.com/results'
            f'?search_query={q.replace(" ","+")}'+sp)

def _channel_url_normalize(url):
    url = url.rstrip('/')
    if 'playlist?' in url: return url
    url = re.sub(
        r'/(featured|shorts|streams|community|about|playlists)$','',url)
    return url+'/videos'

def _is_valid_video_id(vid):
    """Return True if id matches canonical YouTube video-id format."""
    return bool(_YT_VIDEO_ID_RE.fullmatch((vid or '').strip()))

def _fetch_url_info(url, cookie_path, cancel_ev=None):
    opts={'quiet':True,'no_warnings':True,'skip_download':True,
          'cookiefile':Cfg.fix(cookie_path),
          'no_check_certificates':True,'socket_timeout':15}
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            def _ex():
                with yt_dlp.YoutubeDL(opts) as y:
                    return y.extract_info(url,download=False)
            fut=ex.submit(_ex); deadline=time.time()+30
            while not fut.done():
                if cancel_ev and cancel_ev.is_set(): return None
                if time.time()>deadline: return None
                time.sleep(0.2)
            info=fut.result()
        if not info: return None
        vid  =info.get('id','')
        if not _is_valid_video_id(vid): return None
        title=(info.get('title','') or '').strip() or url
        ds   =int(info.get('duration') or 0)
        dur  =info.get('duration_string','')
        if not dur and ds: dur=f'{int(ds)//60}:{int(ds)%60:02d}'
        return {'id':vid,'title':title,
                'channel':info.get('channel') or info.get('uploader','N/A'),
                'url':url,'duration':dur or 'N/A','dur_s':ds,
                'view_count':info.get('view_count')}
    except Exception: return None

def _fetch_multi_urls(urls, cookie_path, cancel_ev=None, progress_cb=None):
    results=[None]*len(urls); done_cnt=[0]; lock=threading.Lock()
    def _fetch_one(i,url):
        if cancel_ev and cancel_ev.is_set(): return
        item=_fetch_url_info(url,cookie_path,cancel_ev)
        with lock:
            results[i]=item; done_cnt[0]+=1
            if progress_cb:
                try: progress_cb(done_cnt[0],len(urls))
                except: pass
    sem=threading.Semaphore(4); threads=[]
    def _worker(i,url):
        with sem: _fetch_one(i,url)
    for i,url in enumerate(urls):
        t=threading.Thread(target=_worker,args=(i,url),daemon=True)
        t.start(); threads.append(t)
    for t in threads: t.join()
    return [r for r in results if r is not None]

def _fetch_channel(url, count, cookie_path, cancel_ev=None):
    url=_channel_url_normalize(url)
    opts={'quiet':True,'no_warnings':True,'extract_flat':True,
          'skip_download':True,'cookiefile':Cfg.fix(cookie_path),
          'ignoreerrors':True,'no_check_certificates':True,
          'socket_timeout':Cfg.SEARCH_SOCKET_TIMEOUT,
          'playlistend':count}
    info=None; cancelled=[False]
    def _ex():
        with yt_dlp.YoutubeDL(opts) as y:
            return y.extract_info(url,download=False)
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut=ex.submit(_ex); deadline=time.time()+Cfg.SEARCH_HARD_TIMEOUT
        while not fut.done():
            if cancel_ev and cancel_ev.is_set():
                cancelled[0]=True; break
            if time.time()>deadline: break
            time.sleep(0.3)
        if not cancelled[0]:
            try: info=fut.result(timeout=1)
            except: pass
    if cancelled[0] or not info: return [],cancelled[0]
    parent_ch=(info.get('channel') or info.get('uploader') or
               info.get('title') or '')
    entries=[]
    if info.get('_type') in ('playlist','channel'):
        entries=info.get('entries') or []
    elif info.get('id'): entries=[info]
    res=[]; seen_ids=set()
    for e in entries:
        if not e or not isinstance(e,dict): continue
        vid=e.get('id',''); title=(e.get('title','') or '').strip()
        if (not vid) or (not title) or (vid in seen_ids): continue
        if not _is_valid_video_id(vid): continue
        seen_ids.add(vid)
        ds=int(e.get('duration') or 0)
        dur=e.get('duration_string','')
        if not dur and ds: dur=f'{int(ds)//60}:{int(ds)%60:02d}'
        u=e.get('url','') or e.get('webpage_url','')
        if u and not u.startswith('http'):
            u=f'https://www.youtube.com/watch?v={u}'
        if not u and vid:
            u=f'https://www.youtube.com/watch?v={vid}'
        ch=(e.get('channel') or e.get('uploader') or
            e.get('channel_id') or parent_ch or 'N/A')
        res.append({'id':vid,'title':title,'channel':ch,
                    'url':u,'duration':dur or 'N/A','dur_s':ds,
                    'view_count':e.get('view_count')})
    res.sort(key=lambda x:(x.get('view_count') is None,
                           -(int(x.get('view_count') or 0))))
    return res,False

def _do_search_raw(url, pool_size, cookie_path):
    opts={'quiet':True,'no_warnings':True,'extract_flat':'in_playlist',
          'skip_download':True,'cookiefile':Cfg.fix(cookie_path),
          'ignoreerrors':True,'no_check_certificates':True,
          'socket_timeout':Cfg.SEARCH_SOCKET_TIMEOUT,
          'playlistend':pool_size}
    info=None
    def _ex():
        with yt_dlp.YoutubeDL(opts) as y:
            return y.extract_info(url,download=False)
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            try: info=ex.submit(_ex).result(timeout=Cfg.SEARCH_HARD_TIMEOUT)
            except FT: return []
    except MaxDownloadsReached: pass
    except DownloadError as e:
        if any(x in str(e) for x in ('403','Forbidden','Sign in')):
            raise CookieError('Cookie expired (403)') from e
        return []
    except Exception: return []
    if not info: return []
    entries=(info.get('entries')
             if info.get('_type') in ('playlist','search') else [info])
    return entries or []

def _filter_entries(entries, count, mode_cfg,
                    saved_ids=None, skip_saved=False):
    res=[]; seen_ids=set(); seen_t=[]; skipped=0
    for e in entries:
        if len(res)>=count: break
        if not e or not isinstance(e,dict): continue
        vid=e.get('id',''); title=(e.get('title','') or '').strip()
        if (not vid) or (not title) or (vid in seen_ids): continue
        if not _is_valid_video_id(vid): continue
        if skip_saved and saved_ids and vid in saved_ids:
            skipped+=1; continue
        ds=int(e.get('duration') or 0)
        if AD_KW.search(title): continue
        if mode_cfg.get('neg_kw') and NEG_KW.search(title): continue
        if _dedup(title,seen_t): continue
        mn=mode_cfg.get('min_dur',0); mx=mode_cfg.get('max_dur')
        if ds>0:
            if mn and ds<mn: continue
            if mx and ds>mx: continue
        seen_ids.add(vid); seen_t.append(title)
        u=e.get('url','') or e.get('webpage_url','')
        if u and not u.startswith('http'):
            u=f'https://www.youtube.com/watch?v={u}'
        if not u and vid:
            u=f'https://www.youtube.com/watch?v={vid}'
        dur=e.get('duration_string','')
        if not dur and ds: dur=f'{int(ds)//60}:{int(ds)%60:02d}'
        res.append({'id':vid,'title':title,
                    'channel':e.get('channel') or e.get('uploader','N/A'),
                    'url':u,'duration':dur or 'N/A','dur_s':ds,
                    'view_count':e.get('view_count')})
    res.sort(key=lambda x:int(x.get('view_count') or 0),reverse=True)
    return res,skipped

def _do_search(url, count, cookie_path, mode_cfg,
               cancel_ev=None, saved_ids=None, skip_saved=False):
    entries=_do_search_raw(url,count*4,cookie_path)
    if cancel_ev and cancel_ev.is_set(): return [],0
    res,skipped=_filter_entries(entries,count,mode_cfg,saved_ids,skip_saved)
    rounds=0
    while len(res)<count*Cfg.SEARCH_FALLBACK_RATIO and rounds<2:
        if cancel_ev and cancel_ev.is_set(): break
        rounds+=1
        entries2=_do_search_raw(url,count*(4+rounds*2),cookie_path)
        res,skipped=_filter_entries(
            entries2,count,mode_cfg,saved_ids,skip_saved)
    return res,skipped


# ══════════════════════════════════════════════════════════════
# BLOCK 9 ── 下载核心（B-2: progress改函数）
# ══════════════════════════════════════════════════════════════
class _StopDownload(Exception): pass

_WARN_SKIP=('PO Token','po_token','subtitles require','missing subtitles',
            'jsc','SABR','Ignoring unsupported')
_SB_RE=re.compile(
    r'captcha|not a robot|automated.{0,30}access|unusual traffic|'
    r'please verify|sign in to confirm|too many request|'
    r'rate.?limit|http error 429|http error 403',re.I)
_VIDEO_EXTS=frozenset({'.mp4','.mkv','.webm','.m4v','.avi','.flv'})
_THUMB_EXTS=frozenset({'.webp','.jpg','.jpeg','.png'})

_FAIL_LABELS = {
    '403': 'Cookie失效/权限受限',
    '429': '请求过频/限流',
    'dl_err': '下载错误',
    'empty': '无有效输出',
    'video_small': '视频文件异常偏小',
    'output_missing': '输出缺失',
    'yt_fail': 'yt-dlp失败',
}

def _classify_fail_reason(reason):
    r = str(reason or '').strip()
    if not r:
        return '未知错误'
    return _FAIL_LABELS.get(r, r[:42])

def _summarize_failed_map(failed_map):
    bucket = {}
    for _vid, rec in (failed_map or {}).items():
        if not isinstance(rec, dict):
            continue
        rs = _classify_fail_reason(rec.get('reason', ''))
        c = int(rec.get('count') or 1)
        bucket[rs] = bucket.get(rs, 0) + max(1, c)
    return sorted(bucket.items(), key=lambda x: x[1], reverse=True)

def _build_retry_items_from_failed_map(failed_map):
    rows = []
    for vid, rec in (failed_map or {}).items():
        if not _is_valid_video_id(vid):
            continue
        rec = rec if isinstance(rec, dict) else {}
        title = str(rec.get('title', '') or '').strip() or f'Failed video {vid}'
        rows.append({
            'id': vid,
            'title': title,
            'url': f'https://www.youtube.com/watch?v={vid}',
            'channel': 'N/A',
            'duration': 'N/A',
            'dur_s': 0,
            'view_count': None,
            '_fail_count': int(rec.get('count') or 0),
        })
    rows.sort(key=lambda x: (-int(x.get('_fail_count') or 0), x.get('title', '')))
    for r in rows:
        r.pop('_fail_count', None)
    return rows


def _do_download(items, cookie_path, save_dir,
                 stop_ev, pause_ev, state, session_dir,
                 log, status, prog_cb=None, flush_cb=None,
                 subtitle_on=False, thumb_on=False, video_on=True,
                 package_on=False, subtitle_split_on=True,
                 subtitle_split_tokens=1200, table_mark_cb=None):

    def _mark(vid,s,r=''):
        if table_mark_cb and vid:
            try: table_mark_cb(vid,s,r)
            except: pass

    cp=Cfg.fix(cookie_path)
    os.makedirs(Cfg.TMP_DIR,exist_ok=True)
    os.makedirs(session_dir,exist_ok=True)
    done=fails=0; stop_why=None; total_bytes=0
    t0=time.time(); sb_consec=0
    n=len(items); done_ids=set(); vid_order=[]; subtitle_parts=[]
    session_name=os.path.basename(session_dir)

    log.write(f'Start {n} videos  FRAGS={Cfg.FRAGS} '
              f'chunk={Cfg.HTTP_CHUNK_MB}MB '
              f'video={"on" if video_on else "off"} '
              f'subtitles={"on" if subtitle_on else "off"} '
              f'sub_split={"on" if subtitle_split_on else "off"} '
              f'thumbs={"on" if thumb_on else "off"} '
              f'pack={"on" if package_on else "off"} '
              f'maxsize={"unlimited" if Cfg.MAX_MB==0 else str(Cfg.MAX_MB)+"MB"}')
    log.write(f'Dir: {session_dir}')
    if flush_cb: flush_cb()

    for idx,item in enumerate(items,1):
        if stop_ev.is_set(): stop_why='user_stop'; break

        vid  =item.get('id','');  title=item.get('title','')[:52]
        url  =item.get('url',''); ch   =item.get('channel','')
        views=_fmt_views(item.get('view_count')) or '-'

        if status: status.downloading(idx,n,title)
        if prog_cb: prog_cb(idx,n,title)
        _mark(vid,'downloading')
        if flush_cb: flush_cb()

        if vid and state.is_done(vid):
            log.write(f'[{idx}/{n}] skip(already downloaded): {title}')
            _mark(vid,'skip','already downloaded')
            if flush_cb: flush_cb(); continue
        if vid and not state.can_retry(vid):
            log.write(f'[{idx}/{n}] skip(too many failures): {title}')
            _mark(vid,'fail','too many failures'); fails+=1
            if flush_cb: flush_cb(); continue

        log.write(f'[{idx}/{n}] {title} | {views} | '
                  f'{item.get("duration","N/A")}')
        shutil.rmtree(Cfg.TMP_DIR,ignore_errors=True)
        os.makedirs(Cfg.TMP_DIR)
        sb_log=[]; last_step=[-1]; last_flush_pct=[-1]

        class _Logger:
            def debug(self_,msg):
                if stop_ev.is_set(): raise _StopDownload()
                if '[download]' in msg:
                    m=re.search(r'(\d+(?:\.\d+)?)%',msg)
                    if m:
                        pct=float(m.group(1)); step=int(pct)
                        if step!=last_step[0]:
                            last_step[0]=step
                            if status:
                                status.update_progress(pct,idx,n,title)
                            bucket=(step//25)*25
                            if bucket!=last_flush_pct[0]:
                                last_flush_pct[0]=bucket
                                if flush_cb: flush_cb()
                if _SB_RE.search(msg): sb_log.append(msg[:80])
            def warning(self_,msg):
                if any(k in msg for k in _WARN_SKIP): return
                if '429' in msg and 'subtitle' in msg.lower():
                    log.write('subtitle rate-limited, skipped'); return
                log.write(f'warn: {msg.strip()[:88]}')
            def error(self_,msg):
                if '429' in msg and 'subtitle' in msg.lower():
                    log.write('subtitle error skipped'); return
                log.write(f'err: {msg.strip()[:88]}')

        opts={
            'quiet':False,'no_warnings':False,'logger':_Logger(),
            'cookiefile':cp,
            'skip_download':(not video_on),
            'concurrent_fragment_downloads':Cfg.FRAGS,
            'http_chunk_size':Cfg.HTTP_CHUNK_MB*1024*1024,
            'writethumbnail':thumb_on,
            'writesubtitles':subtitle_on,
            'writeautomaticsub':subtitle_on,
            'subtitleslangs':(['zh-Hans','zh-Hant','en']
                              if subtitle_on else []),
            'ffmpeg_location':'/usr/bin/ffmpeg',
            'format':('bestvideo[ext=mp4][height<=1080]'
                      '+bestaudio[ext=m4a]/best[ext=mp4]/best'),
            'outtmpl':f'{Cfg.TMP_DIR}/%(id)s__%(title).60B.%(ext)s',
            'windowsfilenames':True,
            'no_check_certificates':True,
            'ignoreerrors':False,'retries':3,'fragment_retries':5,
            'socket_timeout':30,
            'sleep_interval_requests':0.5,'max_sleep_interval':1.2,
            'remote_components':['ejs:github'],
            'playlist_items':'1',
        }
        if Cfg.MAX_MB>0: opts['max_filesize']=Cfg.MAX_MB*1024*1024

        dl_ok=False
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                dl_ok=(ydl.download([url])==0)
        except _StopDownload:
            log.write('force stopped'); stop_why='user_stop'; break
        except DownloadError as e:
            s=str(e).lower()
            if '403' in s or 'forbidden' in s:
                log.write('Cookie expired, please re-export')
                if status: status.error('Cookie expired')
                state.fail(vid,title,'403')
                _mark(vid,'fail','Cookie expired')
                stop_why='403'
                if flush_cb: flush_cb(); break
            elif '429' in s and 'subtitle' in s:
                log.write('subtitle rate-limited, skipped'); dl_ok=True
            elif '429' in s:
                log.write('rate-limited, waiting 45s...')
                time.sleep(45)
                state.fail(vid,title,'429'); fails+=1
                _mark(vid,'fail','rate-limited')
                if flush_cb: flush_cb(); continue
            elif 'no space' in s:
                log.write('Drive full')
                if status: status.error('Drive full')
                stop_why='disk_full'
                if flush_cb: flush_cb(); break
            else:
                log.write(f'dl failed: {str(e)[:100]}')
                state.fail(vid,title,'dl_err'); fails+=1
                _mark(vid,'fail',str(e)[:60])
                if flush_cb: flush_cb(); continue
        except Exception as e:
            if stop_ev.is_set(): stop_why='user_stop'; break
            log.write(f'unexpected: {type(e).__name__}: {e}')
            state.fail(vid,title,type(e).__name__); fails+=1
            _mark(vid,'fail',type(e).__name__)
            if flush_cb: flush_cb(); continue

        if stop_ev.is_set(): stop_why='user_stop'; break

        if sb_log:
            sb_consec+=1
            if sb_consec>=3:
                log.write('multiple access limits, waiting 90s...')
                time.sleep(90); sb_consec=0
        else: sb_consec=0

        if dl_ok:
            tmp_files=[]
            try: tmp_files=os.listdir(Cfg.TMP_DIR)
            except: pass
            if not tmp_files:
                log.write(f'no file (region lock/size limit): {title}')
                state.fail(vid,title,'empty'); fails+=1
                _mark(vid,'fail','no file')
                if flush_cb: flush_cb()
            else:
                vid_files=[]; thumb_files=[]; sub_files=[]; copy_ok=True
                detected_vid=''
                for fn in tmp_files:
                    if '._emb_' in fn: continue
                    src=os.path.join(Cfg.TMP_DIR,fn)
                    dst=os.path.join(session_dir,fn)
                    ext=os.path.splitext(fn)[1].lower()
                    if not detected_vid and '__' in fn:
                        cand=fn.split('__',1)[0]
                        if _is_valid_video_id(cand):
                            detected_vid=cand
                    try:
                        shutil.copy2(src,dst); sz=os.path.getsize(dst)
                        try: os.remove(src)
                        except: pass
                        total_bytes+=sz
                        log.write(f'saved: {fn} ({_fmt_size(sz)})')
                        if ext in _VIDEO_EXTS:
                            vid_files.append((fn,dst,sz))
                        elif ext in _THUMB_EXTS:
                            thumb_files.append((fn,dst,sz))
                        elif ext in {'.vtt','.srt','.ass','.ssa','.lrc'}:
                            sub_files.append((fn,dst,sz))
                    except OSError as e2:
                        log.write(f'save failed: {fn} - {e2}')
                        if ext in _VIDEO_EXTS: copy_ok=False

                if video_on:
                    main_ok=copy_ok and any(sz>100*1024
                                            for _,_,sz in vid_files)
                else:
                    expects=[]
                    if subtitle_on: expects.append(bool(sub_files))
                    if thumb_on:    expects.append(bool(thumb_files))
                    main_ok=copy_ok and bool(expects) and any(expects)

                if main_ok:
                    vid_key=(vid or detected_vid or '')
                    state.done(vid_key,title=title,
                               channel=ch,session=session_name)
                    done+=1
                    if vid_key: done_ids.add(vid_key)
                    vid_order.append((vid_key,title))
                    if subtitle_on and subtitle_split_on and vid_key:
                        try:
                            subtitle_parts.extend(_split_subtitle_files(
                                session_dir,vid_key,idx,title,
                                max_tokens=max(300,int(subtitle_split_tokens)),
                                log=log))
                        except Exception as e_sp:
                            log.write(f'subtitle split failed: {type(e_sp).__name__}')
                    log.write(f'+ done [{done}/{n}]: {title}')
                    _mark(vid_key or vid,'done')
                else:
                    if video_on:
                        log.write(f'x video missing or too small: {title}')
                        state.fail(vid,title,'video_small'); fails+=1
                        _mark(vid,'fail','video too small')
                    else:
                        log.write(f'x output missing (subtitle/thumb): {title}')
                        state.fail(vid,title,'output_missing'); fails+=1
                        _mark(vid,'fail','output missing')
        else:
            log.write(f'x failed: {title}')
            state.fail(vid,title,'yt_fail'); fails+=1
            _mark(vid,'fail','yt-dlp failed')

        if flush_cb: flush_cb()

        # A-10: 视频级暂停，每个视频下完后检查
        if pause_ev.is_set() and not stop_ev.is_set():
            if status: status.paused_after(done,n)
            log.write(f'[paused] {done}/{n} done — click Resume')
            if flush_cb: flush_cb()
            while pause_ev.is_set() and not stop_ev.is_set():
                time.sleep(0.5)
            if not stop_ev.is_set():
                if status: status.resuming()
                log.write('[resumed]')
                if flush_cb: flush_cb()

    try:
        if package_on:
            _package_by_video(session_dir,vid_order,log)
        else:
            _rename_with_index(session_dir,vid_order)
    except: pass
    try: state._save()
    except: pass

    elapsed=time.time()-t0
    spd=(_fmt_size(int(total_bytes/elapsed))+'/s'
         if elapsed>0 and total_bytes>0 else '-')
    label={'403':'stopped:cookie expired',
           'disk_full':'stopped:disk full',
           'user_stop':'user stopped'}.get(stop_why,'all done')
    log.write('='*40)
    log.write(f'{label} | +{done} -{fails} | '
              f'{_fmt_size(total_bytes)} | {spd} | {elapsed:.0f}s')
    log.write('='*40)
    return done,fails,stop_why,done_ids,total_bytes,elapsed,subtitle_parts


# ══════════════════════════════════════════════════════════════
# BLOCK 10 ── PreviewTable
# 修复: _selected_set / .yt-rows-box隔离 / unobserve_all /
#       A-7透明背景 / A-3删发布时间 / C单toggle已存按钮 /
#       删除补全相关代码
# ══════════════════════════════════════════════════════════════
_DRAG_JS = """
<script>
(function(){
  if(!document.getElementById('yt-v331-style-fix')){
    var st=document.createElement('style');
    st.id='yt-v331-style-fix';
    st.textContent=
      '.yt-rows-box .widget-hbox{overflow:hidden !important;}'+
      '.yt-rows-box .widget-box{overflow:hidden !important;}'+
      '.yt-rows-box .widget-html-content{overflow:hidden !important;}'+
      '.yt-rows-box{scrollbar-width:none;-ms-overflow-style:none;'+
      'border-radius:12px;background:rgba(255,255,255,0.02);'+
      'box-shadow:inset 0 1px 0 rgba(255,255,255,0.04),'+
      '0 8px 20px rgba(0,0,0,0.18);backdrop-filter:blur(8px);}'+
      '.yt-rows-box::-webkit-scrollbar{width:0 !important;height:0 !important;}'+
      '.yt-rows-box input[type=checkbox]{cursor:pointer;}';
    document.head.appendChild(st);
  }
  if(window._yt_drag_v331) return;
  window._yt_drag_v331 = true;
  var dragState={
    on:false,startIdx:-1,curIdx:-1,minIdx:-1,maxIdx:-1,targetVal:null,oldSel:''
  };

  function _rowCbs(){
    var c=document.querySelector('.yt-rows-box');
    if(!c) return [];
    return Array.from(c.querySelectorAll('input[type=checkbox]'));
  }
  function _idxFromPoint(x,y){
    var cbs=_rowCbs(); if(!cbs.length) return -1;
    for(var i=0;i<cbs.length;i++){
      var r=cbs[i].getBoundingClientRect();
      if(y>=r.top && y<=r.bottom) return i;
    }
    var best=-1,bestD=1e9;
    for(var j=0;j<cbs.length;j++){
      var r2=cbs[j].getBoundingClientRect();
      var cy=(r2.top+r2.bottom)/2;
      var d=Math.abs(y-cy);
      if(d<bestD){bestD=d;best=j;}
    }
    return best;
  }
  function _getCbInRows(el){
    if(!el) return null;
    var c=document.querySelector('.yt-rows-box');
    if(!c||!c.contains(el)) return null;
    if(el.type==='checkbox') return el;
    if(el.closest){
      var lb=el.closest('label');
      if(lb){var cb=lb.querySelector('input[type=checkbox]');
        if(cb&&c.contains(cb)) return cb;}
    }
    return null;
  }

  document.addEventListener('pointerdown',function(e){
    var cb=_getCbInRows(e.target); if(!cb) return;
    var cbs=_rowCbs(); var idx=cbs.indexOf(cb); if(idx<0) return;
    dragState.on=true; dragState.startIdx=idx; dragState.curIdx=idx;
    dragState.minIdx=idx; dragState.maxIdx=idx;
    dragState.targetVal=!cb.checked;
    dragState.oldSel=document.body.style.userSelect || '';
    document.body.style.userSelect='none';
    cb.checked=dragState.targetVal; e.preventDefault();
  },{capture:true,passive:false});

  document.addEventListener('pointermove',function(e){
    if(!dragState.on) return;
    var idx=_idxFromPoint(e.clientX,e.clientY); if(idx<0) return;
    var cbs=_rowCbs();
    dragState.curIdx=idx;
    if(dragState.minIdx<0||idx<dragState.minIdx) dragState.minIdx=idx;
    if(dragState.maxIdx<0||idx>dragState.maxIdx) dragState.maxIdx=idx;
    var lo=Math.min(dragState.startIdx,idx),hi=Math.max(dragState.startIdx,idx);
    cbs.forEach(function(c,i){if(i>=lo&&i<=hi) c.checked=dragState.targetVal;});
  },{capture:true,passive:true});

  document.addEventListener('pointerup',function(){
    if(!dragState.on){dragState.on=false;return;} dragState.on=false;
    document.body.style.userSelect=dragState.oldSel;
    var lo=(dragState.minIdx>=0)?dragState.minIdx
      :Math.min(dragState.startIdx,dragState.curIdx);
    var hi=(dragState.maxIdx>=0)?dragState.maxIdx
      :Math.max(dragState.startIdx,dragState.curIdx);
    try{google.colab.kernel.invokeFunction(
      '_yt_drag_commit',[lo,hi,dragState.targetVal?1:0],{});}catch(e){}
    dragState.minIdx=-1; dragState.maxIdx=-1;
  },true);

  document.addEventListener('click',function(e){
    if(!_getCbInRows(e.target)) return;
    if(e.detail===0) return; // keep keyboard accessibility (Space/Enter)
    e.preventDefault();
    e.stopPropagation();
  },true);

  document.addEventListener('pointercancel',function(){
    dragState.on=false;
    document.body.style.userSelect=dragState.oldSel;
    dragState.minIdx=-1; dragState.maxIdx=-1;
  },true);
})();
</script>
"""

_AUTO_REFRESH_JS = """
<script>
(function(){
  if(window._yt_auto_timer) clearInterval(window._yt_auto_timer);
  window._yt_auto_timer=setInterval(function(){
    try{google.colab.kernel.invokeFunction('_yt_dl_flush',[],{});}
    catch(e){}
  },2000);
})();
</script>
"""

_ST_CFG={
    'downloading':('#ff9800','>'),
    'done':       ('#4caf50','+'),
    'fail':       ('#f44336','x'),
    'skip':       ('#9e9e9e','-'),
    'saved':      ('#2196f3','v'),
}

def _st_span(st=None,reason=''):
    if not st:
        return '<span style="display:inline-block;width:20px;height:20px"></span>'
    color,icon=_ST_CFG.get(st,('#888','?'))
    tip=(f' title="{escape(str(reason),quote=True)}"'
         if reason else '')
    return (f'<span style="display:inline-block;width:20px;height:20px;'
            f'line-height:20px;text-align:center;border-radius:3px;'
            f'background:{color};color:#fff;font-size:11px;'
            f'font-weight:bold;cursor:default"{tip}>{icon}</span>')

# A-3/A-7: 去掉发布时间列，透明背景
# 列: 28px 1fr 22px 72px 54px
_GRID='28px 1fr 22px 72px 54px'
_UI_FONT_FAMILY=('-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,'
                 '"Noto Sans CJK SC","Noto Sans SC","PingFang SC",'
                 '"Microsoft YaHei",sans-serif')

def _row_html(i, r, st=None, reason=''):
    title_raw=str(r.get('title','') or '')
    ts_raw   =_trim(title_raw,34)
    ch_raw   =_trim(r.get('channel') or 'N/A',24)
    dur_raw  =str(r.get('duration','-') or '-')
    url_raw  =str(r.get('url','#') or '#')
    pu=urlparse(url_raw)
    if pu.scheme.lower() not in ('http','https') or not pu.netloc:
        url_raw='#'
    views_raw=_fmt_views(r.get('view_count'))
    title=escape(title_raw,quote=True)
    ts   =escape(ts_raw,quote=True)
    ch   =escape(ch_raw,quote=True)
    dur  =escape(dur_raw,quote=True)
    url  =escape(url_raw,quote=True)
    # A-7: 透明背景
    bg='rgba(255,255,255,0.03)' if i%2==0 else 'transparent'
    vh=(f'<span style="font-size:12px;color:#ccc">'
        f'{escape(str(views_raw),quote=True)}</span>'
        if views_raw else
        '<span style="color:#555;font-size:11px">-</span>')
    st_s=_st_span(st,reason)
    return (
        f'<div style="display:grid;grid-template-columns:{_GRID};'
        f'gap:0 6px;align-items:center;min-height:52px;'
        f'padding:3px 6px;background:{bg};'
        f'font-family:{_UI_FONT_FAMILY};'
        f'border-bottom:1px solid rgba(255,255,255,0.06)">'
        f'<div style="text-align:center;color:#666;font-size:11px">{i+1}</div>'
        f'<div style="min-width:0;overflow:hidden">'
        f'<a href="{url}" target="_blank" '
        f'style="color:#64b5f6;text-decoration:none;font-size:13px;'
        f'font-weight:500;display:block;white-space:nowrap;'
        f'overflow:hidden;text-overflow:ellipsis" title="{title}">{ts}</a>'
        f'<div style="color:#777;font-size:11px;white-space:nowrap;'
        f'overflow:hidden;text-overflow:ellipsis">{ch}</div>'
        f'</div>'
        f'<div style="text-align:center">{st_s}</div>'
        f'<div style="text-align:right;padding-right:4px">{vh}</div>'
        f'<div style="text-align:center;font-size:12px;color:#aaa">{dur}</div>'
        f'</div>')


class PreviewTable:
    def __init__(self):
        self._items         = []
        self._boxes         = []
        self._content_w     = []
        self._st_states     = []
        self._selected_set  = set()
        self._cb_lock       = threading.Lock()
        self._pending_marks = {}
        self._pending_lock  = threading.Lock()
        self._saved_ids     = set()
        self._is_downloading= False
        self._saved_toggled = False
        self._rows_box = W.VBox(layout=W.Layout(
            width='100%',height='320px',overflow_y='auto',overflow_x='hidden'))
        self.container = W.VBox(layout=W.Layout(width='100%'))

    def set_saved_ids(self,ids): self._saved_ids=set(ids)
    def set_downloading(self,v): self._is_downloading=v

    def render(self,items):
        if self._is_downloading: return
        # 解绑旧 observe，防止幽灵回调
        for old_cb in self._boxes:
            try: old_cb.unobserve_all()
            except: pass
        self._items=[]; self._boxes=[]; self._content_w=[]
        self._st_states=[]; self._saved_toggled=False
        with self._cb_lock: self._selected_set.clear()
        with self._pending_lock: self._pending_marks.clear()

        if not items:
            self._rows_box.children=()
            self.container.children=(
                W.HTML('<div style="padding:20px;text-align:center;'
                       'color:#9e9e9e;font-size:13px">无结果</div>'),)
            return

        self._items=list(items)
        rows=[]
        for i,r in enumerate(items):
            is_saved=bool(r.get('id') and r['id'] in self._saved_ids)
            init_val=not is_saved
            init_st ='saved' if is_saved else None

            cb=W.Checkbox(
                value=init_val,description='',indent=False,
                layout=W.Layout(width='36px',min_width='36px',
                                height='52px',padding='0 2px'))
            if init_val:
                with self._cb_lock: self._selected_set.add(i)

            def _on_change(change,_i=i):
                with self._cb_lock:
                    if change['new']: self._selected_set.add(_i)
                    else:             self._selected_set.discard(_i)
            cb.observe(_on_change,names='value')
            self._boxes.append(cb)
            self._st_states.append((init_st,''))

            cw=W.HTML(
                value=_row_html(i,r,init_st,''),
                layout=W.Layout(flex='1',min_width='0',overflow='hidden'))
            self._content_w.append(cw)
            rows.append(W.HBox(
                [cb,cw],
                layout=W.Layout(width='100%',align_items='center',
                                min_height='52px',overflow='hidden')))

        # 预览行放入专用容器，JS 只扫此容器
        self._rows_box.children=tuple(rows)
        try: self._rows_box.add_class('yt-rows-box')
        except: pass

        # ── 控制区（全在 _rows_box 外部）──────────────────────
        # A-1: description 纯 ASCII
        all_cb=W.Checkbox(
            value=True,description='全选',indent=False,
            layout=W.Layout(width='auto',min_width='72px'),
            tooltip='全选 / 取消全选')
        def _toggle_all(c):
            for b in self._boxes: b.value=c['new']
        all_cb.observe(_toggle_all,names='value')

        # C: 单 toggle 按钮（Saved+ / Saved-）
        self._saved_toggled=False
        btn_saved=W.Button(
            description='已存+',
            layout=W.Layout(width='auto',min_width='90px',height='30px'),
            style={'font_size':'12px','button_color':'#1565c0'},
            tooltip='已存+: 勾选所有已存视频\n已存-: 取消所有已存视频勾选')

        def _toggle_saved(_):
            self._saved_toggled=not self._saved_toggled
            val=self._saved_toggled
            for r,b in zip(self._items,self._boxes):
                if r.get('id','') in self._saved_ids:
                    b.value=val
            btn_saved.description='已存-' if val else '已存+'
            btn_saved.style.button_color='#c62828' if val else '#1565c0'
        btn_saved.on_click(_toggle_saved)

        has_saved=bool(self._saved_ids & {r.get('id','') for r in items})
        note=W.HTML(
            f'<div style="font-size:10px;color:#b0bec5;padding:4px 8px;'
            f'border-bottom:1px solid rgba(255,255,255,0.08)">'
            f'<b style="color:#ccc">{len(items)}</b> 条结果'
            f'{" &nbsp;[v]=已下载(默认不勾选)" if has_saved else ""}'
            f'&nbsp; [>]=下载中 &nbsp;[+]=完成 &nbsp;[x]=失败'
            f'&nbsp; | 拖拽复选框可批量勾选'
            f'</div>')

        header=W.HTML(
            f'<div style="display:grid;grid-template-columns:{_GRID};'
            f'gap:0 6px;font-size:11px;color:#888;'
            f'padding:5px 6px;border-bottom:1px solid rgba(255,255,255,0.15)">'
            f'<div style="text-align:center">#</div>'
            f'<div>标题 / 频道</div>'
            f'<div style="text-align:center">状态</div>'
            f'<div style="text-align:right;padding-right:4px">播放</div>'
            f'<div style="text-align:center">时长</div>'
            f'</div>')

        ctrl=W.HBox(
            [all_cb,
             W.HTML('<span style="font-size:10px;color:#555;'
                    'margin:auto 6px">|</span>'),
             btn_saved],
            layout=W.Layout(align_items='center',margin='3px 0'))

        self.container.children=(note,ctrl,header,self._rows_box)

    def drag_commit(self,lo,hi,target_val):
        lo=max(0,int(lo))
        hi=min(len(self._boxes)-1,int(hi))
        for i in range(lo,hi+1):
            if i<len(self._boxes):
                self._boxes[i].value=bool(target_val)

    def get_selected(self):
        if not self._items: return None
        with self._cb_lock: indices=sorted(self._selected_set)
        return [self._items[i] for i in indices if i<len(self._items)]

    def set_selected_by_ids(self, vid_ids, value=True):
        ids = set(vid_ids or [])
        if not ids:
            return 0
        changed = 0
        for i, it in enumerate(self._items):
            vid = it.get('id', '')
            if vid and vid in ids and i < len(self._boxes):
                self._boxes[i].value = bool(value)
                changed += 1
        return changed

    def mark(self,vid_id,status,reason=''):
        if not vid_id: return
        with self._pending_lock:
            self._pending_marks[vid_id]=(status,reason)

    def apply_pending_marks(self):
        with self._pending_lock:
            marks=dict(self._pending_marks)
            self._pending_marks.clear()
        for vid_id,(st,reason) in marks.items():
            for i,item in enumerate(self._items):
                if item.get('id','')==vid_id:
                    if i<len(self._content_w):
                        self._st_states[i]=(st,reason)
                        self._content_w[i].value=\
                            _row_html(i,item,st,reason)
                    break

    def clear(self):
        for old_cb in self._boxes:
            try: old_cb.unobserve_all()
            except: pass
        self._items=[]; self._boxes=[]; self._content_w=[]
        self._st_states=[]; self._saved_ids=set()
        self._is_downloading=False; self._saved_toggled=False
        with self._cb_lock: self._selected_set.clear()
        with self._pending_lock: self._pending_marks.clear()
        self._rows_box.children=(); self.container.children=()


# ══════════════════════════════════════════════════════════════
# BLOCK 11 ── Dashboard
# ══════════════════════════════════════════════════════════════
class Dashboard:
    def __init__(self):
        self._index            = VideoIndex()
        self._uiq              = _UIQueue()
        self._state            = State(self._index)
        self._table            = PreviewTable()
        self._log              = LiveLog(self._uiq)
        self._status           = StatusBar(self._uiq)
        self._mode_cfg         = MODES['Hot']
        self._mode_name        = 'Hot'
        self._stop_ev          = threading.Event()
        self._pause_ev         = threading.Event()
        self._cancel_search_ev = threading.Event()
        self._w                = {}
        self._acc_mod          = None
        self._run_id           = 0
        self._cur_idx          = 0
        self._cur_total        = 0
        self._last_results     = None
        self._dl_running       = False
        self._force_retry_once = False

    def _register_callbacks(self):
        if not _IN_COLAB: return
        try:
            from google.colab import output as _co
            _co.register_callback('_yt_dl_flush',
                                  lambda: self._flush_queue())
            _co.register_callback(
                '_yt_drag_commit',
                lambda lo,hi,target: self._table.drag_commit(
                    int(lo),int(hi),bool(int(target))))
        except Exception: pass

    def _auto_flush(self):
        try:
            from IPython import get_ipython
            ip=get_ipython()
            if ip and hasattr(ip,'kernel'):
                ip.kernel.io_loop.call_soon_threadsafe(self._flush_queue)
        except Exception: pass

    def _flush_queue(self):
        try: self._table.apply_pending_marks()
        except Exception: pass

        if self._last_results is not None and not self._dl_running:
            try:
                self._table.render(self._last_results)
                self._last_results=None
                try: display(HTML(_DRAG_JS))
                except: pass
            except Exception: pass

        pending,callbacks=self._uiq.drain()
        if 'log'    in pending: self._log._w.value    =pending['log']
        if 'status' in pending: self._status._w.value =pending['status']
        if 'prev_btn' in pending and pending['prev_btn']=='reset':
            if 'prev' in self._w:
                self._w['prev'].disabled=False
                self._w['prev'].description='搜索'
            if 'cancel_prev' in self._w:
                self._w['cancel_prev'].disabled=True

        for cb in callbacks:
            try: cb()
            except Exception as e:
                try: self._log.write(f'[cb err]{type(e).__name__}:{e}')
                except: pass

    def _do_reset_dl_btns(self):
        if 'dl'     in self._w:
            self._w['dl'].disabled=False
            self._w['dl'].description='下载'
        if 'pause'  in self._w:
            self._w['pause'].disabled=True
            self._w['pause'].description='暂停'
        if 'resume' in self._w: self._w['resume'].disabled=True
        if 'stop'   in self._w:
            self._w['stop'].disabled=True
            self._w['stop'].description='终止'
        if 'prev'   in self._w: self._w['prev'].disabled=False
        self._dl_running=False
        self._table.set_downloading(False)

    def _build(self):
        L=W.Layout
        _ACCORDION_PANEL_WIDTH='49%'

        # 模式按钮
        mode_btns=[]
        for mn,mc in MODES.items():
            b=W.Button(description=MODE_LABELS.get(mn,mn),
                       layout=L(width='80px',height='30px'),
                       style={'font_size':'12px',
                               'button_color':mc['color']},
                       tooltip=mc['desc'])
            def _om(_,_n=mn,_c=mc):
                self._mode_cfg=_c; self._mode_name=_n
                self._w['sort'].value=_c['sort']
                self._w['count'].value=_c['count']
                for _b2,_n2 in zip(mode_btns,MODES):
                    _b2.style.button_color=(
                        '#37474f' if _n2==_n else MODES[_n2]['color'])
                self._log.write(
                    f'模式: {MODE_LABELS.get(_n,_n)} — {_c["desc"]}')
                self._flush_queue()
            b.on_click(_om); mode_btns.append(b)

        w_query=W.Textarea(
            placeholder='关键词 / 单个URL / 多个URL / 频道URL',
            description='输入:',
            style={'description_width':'52px'},
            layout=L(width='98%',height='52px'))
        w_sort=W.Dropdown(
            options=[(SORT_LABELS.get(k,k),k) for k in SORT_OPTS.keys()],
            value='views',
            description='排序:',
            style={'description_width':'40px'},
            layout=L(width='155px'),
            tooltip='相关性=YouTube默认  最多播放=按播放量')
        w_count=W.IntSlider(
            value=15,min=1,max=200,step=1,
            description='数量:',
            style={'description_width':'46px'},
            layout=L(width='46%'),continuous_update=False)
        self._w['sort']=w_sort; self._w['count']=w_count

        mod_rows=[]
        for cat,kws in KEYWORD_MODULES.items():
            btns=[]
            for lbl,(kw,cn) in kws.items():
                b2=W.Button(
                    description=lbl,
                    layout=L(width='auto',height='26px'),
                    style={'font_size':'11px'},
                    tooltip=f'{cn}: {kw}')
                def _ok(_b,_kw=kw):
                    yr=datetime.now().year
                    ky=_kw.replace('2026',str(yr)).replace('2025',str(yr-1))
                    w_query.value=ky
                    if self._acc_mod:
                        self._acc_mod.selected_index=None
                b2.on_click(_ok); btns.append(b2)
            mod_rows.append(W.VBox([
                W.HTML(f'<div style="font-size:11px;font-weight:600;'
                       f'color:#aaa;padding:2px 0">{cat}</div>'),
                W.HBox(btns,layout=L(flex_flow='row wrap',
                                     margin='0 0 4px'))]))

        acc_mod=W.Accordion(
            children=[W.VBox([
                W.VBox(mod_rows)
            ],layout=L(padding='2px 4px 4px'))],
            layout=L(width=_ACCORDION_PANEL_WIDTH,margin='2px 0'))
        try:    acc_mod.titles=('固定模块',)
        except: acc_mod.set_title(0,'固定模块')
        acc_mod.selected_index=None
        self._acc_mod=acc_mod
        mod_block=W.Box(
            [acc_mod],
            layout=L(width=_ACCORDION_PANEL_WIDTH,
                     padding='4px 6px',
                     border='1px solid rgba(255,255,255,0.10)',
                     border_radius='8px',
                     background='rgba(255,255,255,0.02)'))

        w_cookie=W.Text(
            value=Cfg.COOKIE,description='Cookie路径:',
            style={'description_width':'52px'},layout=L(width='97%'),
            tooltip='可填文件或文件夹；支持 JSON（含粘贴到 txt 文件的 JSON）自动转换')
        w_save=W.Text(
            value=Cfg.SAVE_DIR,description='保存路径:',
            style={'description_width':'52px'},layout=L(width='97%'),
            tooltip='保存目录（不存在会自动创建）')
        w_maxmb=W.IntSlider(
            value=0,min=0,max=10000,step=100,
            description='大小上限:',
            style={'description_width':'52px'},
            layout=L(width='52%'),continuous_update=False,
            tooltip='单视频最大体积（MB），0=不限')
        w_maxmb_label=W.HTML(
            value='<span style="font-size:11px;color:#4caf50;'
                  'margin-left:6px">不限</span>')
        def _upd_label(c):
            v=c['new']
            if v==0:
                w_maxmb_label.value=(
                    '<span style="font-size:11px;color:#4caf50;'
                    'margin-left:6px">不限</span>')
            else:
                cap=_fmt_size(v*(1<<20))
                w_maxmb_label.value=(
                    f'<span style="font-size:11px;color:#ff9800;'
                    f'margin-left:6px">上限 {cap}</span>')
        w_maxmb.observe(_upd_label,names='value')

        w_subtitle=W.Checkbox(
            value=False,description='下载字幕',indent=False,
            layout=L(width='auto'),
            tooltip='下载字幕 zh-Hans/zh-Hant/en')
        w_sub_quick=W.Button(
            description='字幕模式',
            layout=L(width='72px',height='26px'),
            style={'button_color':'#455a64','font_weight':'600'},
            tooltip='快速切到字幕优先：字幕开/视频关/缩略图关/分包开')
        w_video=W.Checkbox(
            value=True,description='下载视频',indent=False,
            layout=L(width='auto'),
            tooltip='默认开启；关闭后可仅下载字幕/缩略图')
        w_thumb=W.Checkbox(
            value=False,description='下载缩略图',indent=False,
            layout=L(width='auto'),
            tooltip='勾选后输出缩略图文件（jpg/webp/png）')
        w_pack=W.Checkbox(
            value=False,description='单视频分包',indent=False,
            layout=L(width='auto'),
            tooltip='勾选后每个视频相关文件放入独立子文件夹')
        w_skip_saved=W.Checkbox(
            value=True,description='跳过已下载',indent=False,
            layout=L(width='auto'),
            tooltip='跳过已下载视频（推荐开启）')
        w_sub_split=W.Checkbox(
            value=True,description='自动切分字幕',indent=False,
            layout=L(width='auto'),
            tooltip='按Token估算自动切分字幕分片，避免混在一起')
        w_split_tokens=W.IntSlider(
            value=Cfg.SUB_SPLIT_TOKENS,min=300,max=4000,step=100,
            description='字幕Token:',
            style={'description_width':'68px'},
            layout=L(width='55%'),continuous_update=False)
        w_json_index=W.Checkbox(
            value=True,description='导出JSON索引',indent=False,
            layout=L(width='auto'),
            tooltip='导出本次下载结果与字幕分片索引（不影响观感）')
        w_reset_btn=W.Button(
            description='重置记录',button_style='warning',
            layout=L(width='76px'),
            tooltip='清空下载/失败记录（不删除网盘文件）')
        w_rebuild_idx=W.Button(
            description='校验索引',
            layout=L(width='82px'),
            style={'button_color':'#5d6d7e','font_weight':'600'},
            tooltip='按当前保存目录扫描索引_下载结果.json，仅按视频文件校验并重建索引')
        w_retry_failed=W.Button(
            description='失败重试',
            layout=L(width='82px'),
            style={'button_color':'#5d6d7e','font_weight':'600'},
            tooltip='自动勾选当前预览里的失败项，并在本轮下载前清零失败计数')
        w_reset_idx=W.Checkbox(
            value=False,description='含索引',indent=False,
            layout=L(width='auto'),
            tooltip='同时清空已存索引 (.yt_index.json)')
        w_reset_btn.on_click(lambda _:self._on_reset(w_reset_idx.value))
        def _set_sub_mode(_):
            w_subtitle.value=True
            w_video.value=False
            w_thumb.value=False
            w_pack.value=True
            w_sub_split.value=True
        w_sub_quick.on_click(_set_sub_mode)

        acc_set=W.Accordion(
            children=[W.VBox([
                w_cookie,w_save,
                W.HBox([w_maxmb,w_maxmb_label],
                       layout=L(align_items='center')),
                W.HBox([w_subtitle,w_sub_quick,
                        W.HTML('&nbsp;&nbsp;'),
                        w_video,
                        W.HTML('&nbsp;&nbsp;'),
                        w_thumb,
                        W.HTML('&nbsp;&nbsp;'),
                        w_pack,
                        W.HTML('&nbsp;&nbsp;'),
                         w_skip_saved],
                       layout=L(align_items='center')),
                W.HBox([w_sub_split,W.HTML('&nbsp;&nbsp;'),
                        w_split_tokens,W.HTML('&nbsp;&nbsp;'),
                        w_json_index],
                       layout=L(align_items='center')),
                W.HBox([w_reset_btn,W.HTML('&nbsp;'),w_reset_idx,
                        W.HTML('&nbsp;&nbsp;'),w_rebuild_idx,
                        W.HTML('&nbsp;&nbsp;'),w_retry_failed],
                       layout=L(align_items='center')),
            ],layout=L(padding='6px'))],
            layout=L(width=_ACCORDION_PANEL_WIDTH,margin='2px 0'))
        try:    acc_set.titles=('设置',)
        except: acc_set.set_title(0,'设置')
        acc_set.selected_index=None
        set_block=W.Box(
            [acc_set],
            layout=L(width=_ACCORDION_PANEL_WIDTH,
                     padding='4px 6px',
                     border='1px solid rgba(255,255,255,0.10)',
                     border_radius='8px',
                     background='rgba(255,255,255,0.02)'))
        self._w.update({'cookie':w_cookie,'save':w_save,
                        'maxmb':w_maxmb,'subtitle':w_subtitle,
                        'video':w_video,
                        'thumb':w_thumb,'package':w_pack,
                        'skip_saved':w_skip_saved,
                         'sub_split':w_sub_split,
                         'sub_split_tokens':w_split_tokens,
                         'json_index':w_json_index,
                         'rebuild_idx':w_rebuild_idx,
                         'retry_failed':w_retry_failed})

        w_prev=W.Button(description='搜索',
                        layout=L(width='88px',height='34px'),
                        style={'button_color':'#00acc1','font_weight':'600'},
                        tooltip='搜索并显示预览')
        w_dl=W.Button(description='下载',
                      layout=L(width='94px',height='34px'),
                      style={'button_color':'#43a047','font_weight':'600'},
                      tooltip='下载勾选的视频')
        w_pause=W.Button(description='暂停',
                         layout=L(width='76px',height='34px'),disabled=True,
                         style={'button_color':'#bf6f18','font_weight':'600'},
                         tooltip='当前视频下完后暂停')
        w_resume=W.Button(description='继续',
                          layout=L(width='76px',height='34px'),disabled=True,
                          style={'button_color':'#2e7d32','font_weight':'600'},
                          tooltip='继续下载')
        w_stop=W.Button(description='终止',
                        layout=L(width='76px',height='34px'),disabled=True,
                        style={'button_color':'#c0392b','font_weight':'600'},
                        tooltip='终止下载')
        w_refresh=W.Button(description='刷新',
                           layout=L(width='76px',height='34px'),
                           style={'button_color':'#607d8b','font_weight':'600'},
                           tooltip='状态长时间不更新时点击，重启2s定时器')
        w_clear_p=W.Button(description='清空列表',
                           layout=L(width='88px',height='34px'),
                           style={'button_color':'#37474f','font_weight':'600'})
        w_clear_l=W.Button(description='清空日志',
                           layout=L(width='88px',height='34px'),
                           style={'button_color':'#37474f','font_weight':'600'})
        self._w.update({'prev':w_prev,'dl':w_dl,
                        'pause':w_pause,'resume':w_resume,'stop':w_stop})

        def _params():
            Cfg.MAX_MB=w_maxmb.value
            return (w_query.value.strip(),
                    SORT_OPTS.get(w_sort.value,''),
                    w_count.value,
                    Cfg.fix(w_cookie.value.strip()),
                    Cfg.fix(w_save.value.strip()))

        def _on_refresh(_):
            self._flush_queue()
            if _IN_COLAB:
                try: display(HTML(_AUTO_REFRESH_JS))
                except: pass
            self._log.write('自动刷新计时器已重启（2秒）')
            self._flush_queue()

        w_refresh.on_click(_on_refresh)
        w_rebuild_idx.on_click(
            lambda _:self._on_rebuild_index(Cfg.fix(w_save.value.strip()),
                                            w_rebuild_idx))
        w_retry_failed.on_click(lambda _:self._on_retry_failed_prepare())
        w_prev.on_click(
            lambda _:self._on_preview(*_params(),w_prev))
        w_dl.on_click(
            lambda _:self._on_download(
                *_params(),w_dl,w_pause,w_resume,w_stop))
        w_pause.on_click( lambda _:self._on_pause(w_pause,w_resume))
        w_resume.on_click(lambda _:self._on_resume(w_pause,w_resume))
        w_stop.on_click(  lambda _:self._on_stop())
        w_clear_p.on_click(lambda _:self._table.clear())
        w_clear_l.on_click(lambda _:self._log.clear())

        btn_row=W.HBox(
            [w_prev,w_dl,
             W.HTML('<span style="width:8px;display:inline-block"></span>'),
             w_pause,w_resume,w_stop,
             W.HTML('<span style="width:8px;display:inline-block"></span>'),
             w_refresh,w_clear_p,w_clear_l],
            layout=L(margin='6px 0',flex_flow='row wrap',
                     align_items='center'))
        btn_panel=W.Box(
            [btn_row],
            layout=L(width='100%',
                     padding='4px 8px',
                     border='1px solid rgba(255,255,255,0.10)',
                     border_radius='8px',
                     background='rgba(255,255,255,0.02)'))

        w_scroll=W.Box(
            [self._table.container],
            layout=L(width='100%',height='420px',overflow='hidden',
                      border='1px solid rgba(255,255,255,0.12)',
                      border_radius='8px',
                      box_shadow='0 6px 16px rgba(0,0,0,0.18)'))

        def _sep(t):
            return W.HTML(
                f'<div style="font-size:10px;color:#b0bec5;'
                f'border-bottom:1px solid rgba(255,255,255,0.18);'
                f'padding:2px 0;margin:5px 0 2px">{t}</div>')

        log_acc=W.Accordion(
            children=[self._log.widget()],
            layout=L(width='100%',margin='2px 0'))
        try:    log_acc.titles=('日志',)
        except: log_acc.set_title(0,'日志')
        log_acc.selected_index=0
        log_panel=W.Box(
            [log_acc],
            layout=L(width='100%',
                     padding='3px 6px',
                     border='1px solid rgba(255,255,255,0.10)',
                     border_radius='8px',
                     background='rgba(255,255,255,0.02)'))

        note=W.HTML(
            '<div style="font-size:11px;color:#cfd8dc;'
            'background:rgba(255,255,255,0.06);'
            'border:1px solid rgba(255,255,255,0.1);'
            'border-radius:8px;padding:6px 10px;margin:4px 0;'
            'box-shadow:0 4px 12px rgba(0,0,0,0.14);'
            'backdrop-filter:blur(7px)">'
            '支持：关键词 / 单URL / 多URL / 频道URL'
            '&nbsp;|&nbsp;[v]=已下载(默认不勾选)&nbsp;'
            '[>]=下载中&nbsp;[+]=完成&nbsp;[x]=失败'
            '&nbsp;|&nbsp;拖拽复选框可批量勾选'
            '&nbsp;|&nbsp;视频/字幕/缩略图/分包可在设置中勾选'
            '</div>')
        panel_row=W.HBox(
            [acc_mod,acc_set],
            layout=L(width='100%',justify_content='space-between',
                     align_items='flex-start'))

        return W.VBox([
            W.HTML('<div style="font-size:15px;font-weight:600;'
                   'margin:4px 0 6px;color:#ddd">'
                   'YouTube 下载器 '
                   '<span style="font-size:11px;color:#9e9e9e;'
                   'font-weight:400">v331</span></div>'),
            _sep('模式'),
            W.HBox(mode_btns,layout=L(margin='0 0 4px')),
            _sep('搜索'),
            w_query,
            W.HBox([w_sort,W.HTML('&nbsp;'),w_count]),
            panel_row,btn_panel,note,
            self._status.widget(),
            _sep('预览'),
            w_scroll,log_panel,
        ],layout=W.Layout(
             border='1px solid rgba(255,255,255,0.12)',
             padding='12px 14px',width='99%',
             border_radius='10px',
             box_shadow='0 8px 20px rgba(0,0,0,0.20)'))

    # ── 搜索预览 ─────────────────────────────────────────────
    def _on_preview(self,query,sort,count,cookie,save,w_prev):
        self._flush_queue()
        if not query:
            self._log.write('请输入关键词或URL')
            self._status.error('输入为空')
            self._flush_queue(); return
        ok,msg=_mount_drive()
        if not ok:
            self._log.write(f'Drive: {msg}')
            self._status.error(f'Drive 挂载失败: {msg}')
            self._flush_queue(); return
        try:
            cookie_file=_resolve_cookie_file(cookie,create_dir=True)
            found=_check_cookie(cookie_file)
        except CookieError as e:
            self._log.write(f'Cookie 错误: {e}')
            self._status.error(f'Cookie: {e}')
            self._flush_queue(); return
        self._log.write(f'Cookie 文件: {cookie_file}')
        self._log.write(f'Cookie 关键字段: {", ".join(found)}')

        itype,idata=_parse_input(query)
        skip_saved=self._w['skip_saved'].value  # 线程外读取

        self._cancel_search_ev.set()
        self._cancel_search_ev=threading.Event()
        cancel_ev=self._cancel_search_ev

        w_prev.disabled=True
        w_prev.description='搜索中...'
        self._flush_queue()

        def _search():
            res=[]; mode=self._mode_name; skipped=0
            try:
                _mount_drive()
                saved=(self._index.get_all_ids()|
                       self._state.get_dl_set())
                self._log.write(
                    f'已存记录: {len(saved)}  '
                    f'{"跳过已存" if skip_saved else "包含已存"}')

                if itype=='channel_multi_warn':
                    self._log.write(
                        f'检测到 {len(idata)} 个频道URL，使用第1个')
                    self._status.fetching_channel(idata[0])
                    res,cancelled=_fetch_channel(
                        idata[0],count,cookie_file,cancel_ev)
                    if cancelled:
                        self._status.cancelled(); return
                    mode='频道'

                elif itype=='keyword':
                    self._status.searching(idata)
                    self._log.write(f'关键词: {idata}')
                    url=_build_url(idata,sort)
                    res,skipped=_do_search(
                        url,count,cookie_file,self._mode_cfg,cancel_ev,
                        saved_ids=saved if skip_saved else None,
                        skip_saved=skip_saved)
                    mode=self._mode_name

                elif itype=='single_url':
                    self._status.searching(idata)
                    self._log.write(f'读取URL: {_trim(idata,60)}')
                    item=_fetch_url_info(idata,cookie_file,cancel_ev)
                    res=[item] if item else []
                    mode='URL'

                elif itype=='multi_url':
                    self._status.fetching_urls(len(idata))
                    self._log.write(f'读取 {len(idata)} 个URL...')
                    def _prog(d,t):
                        self._log.write(f'  {d}/{t}')
                        self._auto_flush()
                    res=_fetch_multi_urls(idata,cookie_file,cancel_ev,_prog)
                    mode=f'{len(res)}个URL'

                elif itype=='channel':
                    self._status.fetching_channel(idata)
                    self._log.write(f'频道: {_trim(idata,60)}')
                    res,cancelled=_fetch_channel(
                        idata,count,cookie_file,cancel_ev)
                    if cancelled:
                        self._status.cancelled(); return
                    mode='频道'

                if cancel_ev.is_set():
                    self._status.cancelled(); return

                self._table.set_saved_ids(saved)
                self._last_results=res

                if res:
                    self._log.write(
                        f'找到 {len(res)} 条（{mode}）'
                        +(f'  已跳过 {skipped} 条已下载' if skipped else ''))
                    self._status.found(len(res),mode,skipped)
                else:
                    self._log.write(
                        '没有结果'
                        +(f'（跳过 {skipped}）' if skipped else ''))
                    self._status.error('没有结果')

            except CookieError as e:
                self._log.write(f'Cookie 失效: {e}')
                self._status.error(f'Cookie 失效: {e}')
            except Exception as e:
                self._log.write(f'搜索异常: {type(e).__name__}: {e}')
                self._status.error(f'搜索异常: {type(e).__name__}')
            finally:
                self._uiq.put('prev_btn','reset')
                self._auto_flush()

        threading.Thread(target=_search,daemon=True).start()

    # ── 开始下载 ─────────────────────────────────────────────
    def _on_download(self,query,sort,count,cookie,save,
                     w_dl,w_pause,w_resume,w_stop):
        self._last_results=None
        self._flush_queue()

        subtitle_on =self._w['subtitle'].value
        video_on    =self._w['video'].value
        thumb_on    =self._w['thumb'].value
        package_on  =self._w['package'].value
        skip_saved  =self._w['skip_saved'].value
        sub_split_on=bool(self._w['sub_split'].value)
        sub_split_tokens=int(self._w['sub_split_tokens'].value)
        json_index_on=bool(self._w['json_index'].value)
        if (not video_on) and (not subtitle_on) and (not thumb_on):
            self._log.write('请至少勾选一个下载输出选项')
            self._status.error('无可下载输出')
            self._flush_queue(); return
        search_first=False
        itype,idata =_parse_input(query)

        if itype in ('single_url','multi_url','channel',
                     'channel_multi_warn'):
            selected=self._table.get_selected()
            if selected:
                items=selected
            elif itype=='single_url':
                items=[{'id':'','title':_trim(idata,52),'url':idata,
                        'channel':'','duration':'N/A','dur_s':0,
                        'view_count':None}]
            else:
                self._log.write('请先搜索并生成预览')
                self._flush_queue(); return
        else:
            selected=self._table.get_selected()
            if selected is None:
                if not query:
                    self._log.write('请输入关键词或URL')
                    self._flush_queue(); return
                items=None; search_first=True
            elif len(selected)==0:
                self._log.write('未勾选任何视频')
                self._status.error('请至少勾选1条视频')
                self._flush_queue(); return
            else:
                items=list(selected)

        w_dl.disabled=True;   w_dl.description='下载中...'
        w_pause.disabled=False; w_pause.description='暂停'
        w_resume.disabled=True
        w_stop.disabled=False;  w_stop.description='终止'

        self._run_id+=1
        my_run_id=self._run_id
        self._stop_ev =threading.Event()
        self._pause_ev=threading.Event()
        stop_ev =self._stop_ev
        pause_ev=self._pause_ev
        self._dl_running=True
        self._table.set_downloading(True)

        def _guarded_reset():
            if self._run_id==my_run_id:
                self._do_reset_dl_btns()

        # B-2: 用具名函数替代有歧义的 lambda
        def _prog(idx,n,_title):
            self._cur_idx  =idx
            self._cur_total=n

        def _run():
            nonlocal items
            try:
                ok,msg=_mount_drive()
                self._log.write(f'Drive: {msg}'); self._auto_flush()
                if not ok:
                    self._status.error(f'Drive 挂载失败: {msg}')
                    self._auto_flush(); return
                try:
                    cookie_file=_resolve_cookie_file(cookie,create_dir=True)
                    found=_check_cookie(cookie_file)
                    self._log.write(f'Cookie 文件: {cookie_file}')
                    self._log.write(f'Cookie OK: {found}')
                    self._auto_flush()
                except CookieError as e:
                    self._log.write(f'Cookie 失效: {e}')
                    self._status.error(f'Cookie: {e}')
                    self._auto_flush(); return

                if search_first:
                    self._log.write('未检测到预览，先自动搜索...')
                    surl=_build_url(idata,sort)
                    if not surl:
                        self._log.write('输入无效'); return
                    try:
                        saved=(self._index.get_all_ids()|
                               self._state.get_dl_set())
                        res,_sk=_do_search(
                            surl,count,cookie_file,self._mode_cfg,
                            saved_ids=saved if skip_saved else None,
                            skip_saved=bool(skip_saved))
                        if not res:
                            self._log.write('未找到可下载结果')
                            self._auto_flush(); return
                        # B-3: put_cb 保证 render 在主线程，
                        #       time.sleep 给主线程时间执行
                        res_copy=res[:]
                        def _do_render():
                            self._table.set_saved_ids(saved)
                            self._table.set_downloading(False)
                            self._table.render(res_copy)
                            self._table.set_downloading(True)
                            try: display(HTML(_DRAG_JS))
                            except: pass
                        self._uiq.put_cb(_do_render)
                        self._auto_flush()
                        time.sleep(0.4)
                        items=res
                    except CookieError as e:
                        self._log.write(f'Cookie 失效: {e}')
                        self._auto_flush(); return

                sd=_make_session_dir(
                    save,self._mode_name,query,len(items))
                self._log.write(
                    f'开始下载 {len(items)} 条视频 → {sd}')
                self._auto_flush()

                if self._force_retry_once:
                    retry_ids = {x.get('id', '') for x in items if x.get('id', '')}
                    if retry_ids:
                        self._state.clear_failed(retry_ids)
                        self._log.write(f'失败重试模式：已清零 {len(retry_ids)} 条失败计数')
                    self._force_retry_once = False

                done,fails,sw,done_ids,tb,elapsed,subtitle_parts=_do_download(
                    items,cookie_file,save,
                    stop_ev,pause_ev,
                    self._state,sd,
                    self._log,self._status,
                    prog_cb     =_prog,
                    flush_cb    =self._auto_flush,
                    subtitle_on =subtitle_on,
                    video_on    =video_on,
                    thumb_on    =thumb_on,
                    package_on  =package_on,
                    subtitle_split_on=sub_split_on,
                    subtitle_split_tokens=sub_split_tokens,
                    table_mark_cb=self._table.mark)

                self._index.invalidate()
                sl_lbl=next(
                    (SORT_LABELS.get(k,k) for k,v in SORT_OPTS.items()
                     if v==sort), sort)
                _write_index_txt(
                    sd,self._mode_name,query,sl_lbl,items,done_ids)
                if json_index_on:
                    _write_index_json(
                        sd,self._mode_name,query,sl_lbl,items,done_ids,
                        subtitle_parts=subtitle_parts)
                if sw=='user_stop': self._status.stopped(done,fails)
                else:               self._status.done(done,fails,tb,elapsed)

                failed_sum = _summarize_failed_map(self._state.get_failed_map())
                if failed_sum:
                    top = ' | '.join([f'{k}:{v}' for k, v in failed_sum[:5]])
                    self._log.write(f'失败分类汇总: {top}')

            except Exception:
                self._log.write('下载崩溃:')
                self._log.write(traceback.format_exc()[-600:])
                self._status.error('下载崩溃，请看日志')
            finally:
                self._uiq.put_cb(_guarded_reset)
                self._auto_flush()

        threading.Thread(target=_run,daemon=True).start()

    # ── A-10: 暂停/继续/终止（即时按钮反馈）──────────────────
    def _on_pause(self,w_pause,w_resume):
        self._pause_ev.set()
        w_pause.disabled =True
        w_resume.disabled=False
        # 立即更新状态栏，不等线程
        try:
            self._status._w.value=_sb(
                'pause','||',
                f'当前视频结束后暂停 '
                f'({self._cur_idx}/{self._cur_total})')
        except Exception: pass
        self._log.write(
            f'[暂停] 当前视频结束后暂停 '
            f'({self._cur_idx}/{self._cur_total})')
        self._flush_queue()

    def _on_resume(self,w_pause,w_resume):
        self._pause_ev.clear()
        w_pause.disabled =False
        w_resume.disabled=True
        try:
            self._status._w.value=_sb('ok','>','继续下载中...')
        except Exception: pass
        self._log.write('[继续]')
        self._flush_queue()

    def _on_stop(self):
        self._stop_ev.set()
        self._pause_ev.clear()
        if 'stop' in self._w:
            self._w['stop'].disabled   =True
            self._w['stop'].description='终止中'
        try:
            self._status._w.value=_sb('stop','[]','正在终止（当前分片后停止）...')
        except Exception: pass
        self._log.write('[终止] 当前分片完成后停止（非强制秒停）')
        self._flush_queue()

    def _on_reset(self,clear_index=False):
        self._state.reset(clear_index=clear_index)
        self._table.clear()
        self._last_results=None
        msg=('已清空记录（不删除网盘文件）'
             +('（含索引）' if clear_index else ''))
        self._log.write(msg)
        self._status.idle()
        self._flush_queue()

    def _on_retry_failed_prepare(self):
        failed = self._state.get_failed_map()
        self._force_retry_once = False
        if not failed:
            self._log.write('失败重试：当前无失败记录')
            self._status.idle()
            self._flush_queue()
            return
        failed_ids = {vid for vid in failed.keys() if _is_valid_video_id(vid)}
        if not failed_ids:
            self._log.write('失败重试：失败记录中无有效视频ID')
            self._status.error('无有效失败项')
            self._flush_queue()
            return
        cnt = self._table.set_selected_by_ids(failed_ids, value=True)
        if cnt <= 0:
            retry_items = _build_retry_items_from_failed_map(
                {k: v for k, v in failed.items() if k in failed_ids})
            if not retry_items:
                self._log.write('失败重试：失败记录中无有效视频ID')
                self._status.error('无有效失败项')
                self._flush_queue()
                return
            saved = (self._index.get_all_ids() | self._state.get_dl_set())
            self._table.set_saved_ids(saved)
            self._table.set_downloading(False)
            self._table.render(retry_items)
            self._last_results = retry_items
            cnt = self._table.set_selected_by_ids(failed_ids, value=True)
            if cnt <= 0:
                self._log.write('失败重试：重试列表生成成功，但未选中可下载项')
                self._status.error('无可重试项')
                self._flush_queue()
                return
            self._log.write(f'失败重试：已生成重试列表并勾选 {cnt} 条，点击“下载”开始重试')
            self._status.idle()
            try: display(HTML(_DRAG_JS))
            except: pass
        else:
            self._log.write(f'失败重试：已勾选 {cnt} 条，点击“下载”开始重试')
            self._status.idle()
        self._force_retry_once = True
        self._flush_queue()

    def _on_rebuild_index(self, save_dir, btn=None):
        if self._dl_running:
            self._log.write('下载进行中，暂不支持索引校验')
            self._status.error('请等待下载结束后再校验索引')
            self._flush_queue()
            return
        if btn is not None:
            btn.disabled = True
            btn.description = '校验中...'
        self._log.write('开始校验索引（手动触发）...')
        self._flush_queue()

        def _run():
            try:
                ok, msg = _mount_drive()
                if not ok:
                    self._log.write(f'Drive: {msg}')
                    self._status.error(f'Drive 挂载失败: {msg}')
                    return
                rebuilt, stats = _rebuild_index_from_sessions(
                    save_dir, Cfg.INDEX)
                rebuilt_ids = set(rebuilt.keys())
                self._index.replace_all(rebuilt)
                self._state.replace_downloaded_ids(rebuilt_ids)
                self._log.write(
                    f'索引校验完成: 会话{stats["sessions"]} 记录{stats["videos"]} '
                    f'保留{stats["kept"]} 移除{stats["dropped"]}')

                def _refresh_table():
                    saved = (self._index.get_all_ids() |
                             self._state.get_dl_set())
                    self._table.set_saved_ids(saved)
                    if (not self._dl_running) and self._table._items:
                        self._table.render(list(self._table._items))
                        try:
                            display(HTML(_DRAG_JS))
                        except Exception:
                            pass
                self._uiq.put_cb(_refresh_table)
                self._status.idle()
            except Exception as e:
                self._log.write(f'索引校验异常: {type(e).__name__}: {e}')
                self._status.error(f'索引校验异常: {type(e).__name__}')
            finally:
                def _restore_btn():
                    if btn is not None:
                        btn.disabled = False
                        btn.description = '校验索引'
                self._uiq.put_cb(_restore_btn)
                self._auto_flush()

        threading.Thread(target=_run, daemon=True).start()

    def launch(self):
        ui=self._build()
        display(ui)
        self._register_callbacks()
        if _IN_COLAB:
            try: display(HTML(_AUTO_REFRESH_JS))
            except: pass
        try: display(HTML(_DRAG_JS))
        except: pass
        self._log.write(
            'v331 已就绪 — 启动时已检查 Drive 挂载')
        self._log.write(
            f'Cookie默认目录: {Cfg.COOKIE_DIR}（不存在会自动创建）')
        self._log.write(
            f'状态文件目录: {Cfg.META_DIR}')
        self._log.write(
            f'FRAGS={Cfg.FRAGS}  chunk={Cfg.HTTP_CHUNK_MB}MB  '
            f'大小上限={"不限" if Cfg.MAX_MB==0 else str(Cfg.MAX_MB)+"MB"}')
        self._status.idle()
        self._flush_queue()


# ══════════════════════════════════════════════════════════════
# BLOCK 12 ── 启动
# ══════════════════════════════════════════════════════════════
try:
    _INSTANCE._stop_ev.set()
    _INSTANCE._pause_ev.clear()
    try: _INSTANCE._cancel_search_ev.set()
    except: pass
except Exception:
    pass

_colab_preflight()
_INSTANCE=Dashboard()
_INSTANCE.launch()
print('v331 已就绪')
