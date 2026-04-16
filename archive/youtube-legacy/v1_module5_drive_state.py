# ══════════════════════════════════════════════════════════════
# MODULE 5 — Drive / State 延迟加载（验证保留）
# 解决: P0-5 确认无回归
# ══════════════════════════════════════════════════════════════

class VideoIndex:
    """
    ★ P0-5: __init__ 不碰 Drive，所有 IO 在首次实际使用时触发
    """
    def __init__(self):
        self._path  = None      # 延迟到 Drive 挂载后才设置
        self._lock  = threading.Lock()
        self._cache = None      # None = 未加载；dict = 已加载

    def _ensure_path(self):
        if self._path is None:
            self._path = Cfg.fix(Cfg.INDEX)

    def _read_raw(self):
        self._ensure_path()
        try:
            if not os.path.exists(self._path):
                return {'updated': '', 'videos': {}}
            with open(self._path, encoding='utf-8') as f:
                d = json.load(f)
            if isinstance(d.get('videos'), dict):
                return d
        except Exception:
            pass
        return {'updated': '', 'videos': {}}

    def load(self):
        """首次调用时真正读取 Drive 文件"""
        with self._lock:
            if self._cache is None:
                self._cache = self._read_raw()['videos']
            return dict(self._cache)

    def invalidate(self):
        with self._lock:
            self._cache = None

    def write(self, vid, title, channel, session=''):
        if not vid:
            return
        with self._lock:
            try:
                self._ensure_path()
                os.makedirs(os.path.dirname(self._path), exist_ok=True)
                raw = self._read_raw()
                raw['videos'][vid] = {
                    'title':    title,
                    'channel':  channel,
                    'saved_at': datetime.now().isoformat(),
                    'session':  session,
                }
                raw['updated'] = datetime.now().isoformat()
                tmp = self._path + '.tmp'
                with open(tmp, 'w', encoding='utf-8') as f:
                    json.dump(raw, f, ensure_ascii=False, indent=2)
                os.replace(tmp, self._path)
                if self._cache is not None:
                    self._cache[vid] = raw['videos'][vid]
            except Exception:
                pass

    def get_all_ids(self):
        return set(self.load().keys())

    def rebuild_from_state(self, dl_set):
        """从 State._dl 重建索引（仅在 Drive 已挂载时调用）"""
        with self._lock:
            try:
                self._ensure_path()
                raw = self._read_raw()
                for vid in dl_set:
                    if vid not in raw['videos']:
                        raw['videos'][vid] = {
                            'title': '', 'channel': '',
                            'saved_at': '', 'session': 'rebuilt'}
                raw['updated'] = datetime.now().isoformat()
                tmp = self._path + '.tmp'
                with open(tmp, 'w', encoding='utf-8') as f:
                    json.dump(raw, f, ensure_ascii=False, indent=2)
                os.replace(tmp, self._path)
                self._cache = raw['videos']
            except Exception:
                pass


class State:
    """
    ★ P0-5: __init__ 不读 Drive，_ensure_loaded() 懒加载
    ★ get_dl_set() 确保触发 _ensure_loaded()
    """
    def __init__(self, index: VideoIndex):
        self._p      = None     # 延迟设置
        self._dl     = set()
        self._fail   = {}
        self._index  = index
        self._loaded = False
        self._lock   = threading.Lock()

    def _ensure_loaded(self):
        """首次调用时才真正读取 Drive 文件"""
        with self._lock:
            if self._loaded:
                return
            self._loaded = True
            try:
                self._p = Cfg.fix(Cfg.STATE)
                if not os.path.exists(self._p):
                    return
                with open(self._p, encoding='utf-8') as f:
                    d = json.load(f)
                self._dl   = set(d.get('downloaded', []))
                self._fail = d.get('failed', {})
            except Exception:
                pass

    def _save(self):
        try:
            if self._p is None:
                self._p = Cfg.fix(Cfg.STATE)
            os.makedirs(os.path.dirname(self._p), exist_ok=True)
            tmp = self._p + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump({
                    'downloaded': list(self._dl),
                    'failed':     self._fail,
                    'updated':    datetime.now().isoformat()
                }, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self._p)
        except Exception:
            pass

    def done(self, v, title='', channel='', session=''):
        self._ensure_loaded()
        self._dl.add(v)
        self._fail.pop(v, None)
        self._save()
        self._index.write(v, title, channel, session)

    def fail(self, v, t, r):
        self._ensure_loaded()
        x = self._fail.setdefault(v, {'title': t, 'count': 0})
        x['reason'] = r
        x['count'] += 1

    def is_done(self, v):
        self._ensure_loaded()
        return v in self._dl

    def can_retry(self, v):
        self._ensure_loaded()
        return self._fail.get(v, {}).get('count', 0) < 3

    def get_dl_set(self):
        """★ 确保触发懒加载后返回 _dl 的副本"""
        self._ensure_loaded()
        return set(self._dl)

    def reset(self, clear_index=False):
        self._ensure_loaded()
        self._dl.clear()
        self._fail.clear()
        self._save()
        if clear_index:
            try:
                p = Cfg.fix(Cfg.INDEX)
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass
            self._index.invalidate()