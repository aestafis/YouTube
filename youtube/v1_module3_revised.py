# ══════════════════════════════════════════════════════════════
# MODULE 3 REVISED — SearchEnricher
# 修复: widget修改走队列 / 时序问题 / 重复key
# ══════════════════════════════════════════════════════════════

def _fetch_video_meta(vid, cookie_path, timeout=12):
    if not vid:
        return None
    url = f'https://www.youtube.com/watch?v={vid}'
    opts = {
        'quiet':                 True,
        'no_warnings':           True,
        'skip_download':         True,   # 只有一个，无重复
        'cookiefile':            Cfg.fix(cookie_path),
        'no_check_certificates': True,
        'socket_timeout':        10,
    }
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            def _ex():
                with yt_dlp.YoutubeDL(opts) as y:
                    return y.extract_info(url, download=False)
            fut      = ex.submit(_ex)
            deadline = time.time() + timeout
            while not fut.done():
                if time.time() > deadline:
                    return None
                time.sleep(0.15)
            info = fut.result(timeout=1)
        if not info:
            return None
        return {
            'upload_date': info.get('upload_date', ''),
            'channel':     (info.get('channel') or
                            info.get('uploader') or ''),
            'view_count':  info.get('view_count'),
        }
    except Exception:
        return None


class SearchEnricher:

    def __init__(self):
        self._cancel_ev = threading.Event()
        self._thread    = None

    def cancel(self):
        self._cancel_ev.set()

    def is_running(self):
        return self._thread is not None and self._thread.is_alive()

    def start(self, items, cookie_path,
              enqueue_fn,    # table.enqueue_enrichment(i, updated)
              on_done,       # callable(n_enriched)
              enabled=True):
        """
        ★ 修复: 补全结果通过 enqueue_fn 入队，不直接改 widget
        ★ 修复: 去掉 time.sleep(0.1) 启动延迟
        """
        self.cancel()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1)

        if not enabled or not items:
            if on_done:
                try: on_done(0)
                except: pass
            return

        self._cancel_ev = threading.Event()
        cancel_ev       = self._cancel_ev
        items_snapshot  = list(items)   # 快照，防止外部修改

        def _run():
            sem            = threading.Semaphore(3)
            enriched_count = [0]
            count_lock     = threading.Lock()
            threads        = []

            def _enrich_one(i, item):
                with sem:
                    if cancel_ev.is_set():
                        return
                    vid  = item.get('id', '')
                    meta = _fetch_video_meta(vid, cookie_path)
                    if cancel_ev.is_set():
                        return
                    if meta:
                        updated = dict(item)
                        if meta.get('upload_date'):
                            updated['upload_date'] = meta['upload_date']
                        if meta.get('channel'):
                            updated['channel'] = meta['channel']
                        if meta.get('view_count') is not None:
                            updated['view_count'] = meta['view_count']
                        with count_lock:
                            enriched_count[0] += 1
                        # ★ 通过队列，不直接改 widget
                        try:
                            enqueue_fn(i, updated)
                        except Exception:
                            pass
                    # 间隔，避免 429
                    time.sleep(0.3)

            # ★ 直接启动所有线程，sem 控制并发，无额外延迟
            for i, item in enumerate(items_snapshot):
                if cancel_ev.is_set():
                    break
                t = threading.Thread(
                    target=_enrich_one,
                    args=(i, item),
                    daemon=True)
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

            if not cancel_ev.is_set():
                try:
                    on_done(enriched_count[0])
                except Exception:
                    pass

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()