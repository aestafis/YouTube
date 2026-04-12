# ══════════════════════════════════════════════════════════════
# MODULE 3 — 搜索补全详情
# 解决: P1-1(发布时间) / P1-2(频道名N/A)
# ══════════════════════════════════════════════════════════════

def _fetch_video_meta(vid, cookie_path, timeout=12):
    """
    对单个 video_id 获取完整元数据（upload_date / channel / view_count）
    使用轻量级选项，只拿基础字段，不解析格式列表
    返回 dict 或 None
    """
    if not vid:
        return None
    url = f'https://www.youtube.com/watch?v={vid}'
    opts = {
        'quiet':              True,
        'no_warnings':        True,
        'skip_download':      True,
        'cookiefile':         Cfg.fix(cookie_path),
        'no_check_certificates': True,
        'socket_timeout':     10,
        # 不解析格式，只要基础信息
        'extract_flat':       False,
        'skip_download':      True,
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
    """
    搜索结果补全器：
    - 搜索完成后异步补全 upload_date / channel
    - 并发数=3，间隔 0.3s，避免 429
    - 每补全一个立即回调更新预览行
    - 支持外部取消
    """

    def __init__(self):
        self._cancel_ev = threading.Event()
        self._thread    = None
        self._lock      = threading.Lock()

    def cancel(self):
        self._cancel_ev.set()

    def is_running(self):
        return self._thread is not None and self._thread.is_alive()

    def start(self, items, cookie_path, on_item_enriched, on_done,
              enabled=True):
        """
        items           : list of dict，来自搜索结果
        cookie_path     : str
        on_item_enriched: callable(index, updated_item)，每补全一个调用一次
        on_done         : callable(total_enriched)，全部完成后调用
        enabled         : bool，对应 w_enrich.value
        """
        # 先取消上一次未完成的补全
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

        def _run():
            sem            = threading.Semaphore(3)  # 并发数=3
            enriched_count = [0]
            result_lock    = threading.Lock()
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
                        with result_lock:
                            enriched_count[0] += 1
                        # 立即回调，增量更新预览行
                        try:
                            on_item_enriched(i, updated)
                        except Exception:
                            pass
                    # 间隔 0.3s，避免 429
                    time.sleep(0.3)

            for i, item in enumerate(items):
                if cancel_ev.is_set():
                    break
                t = threading.Thread(
                    target=_enrich_one,
                    args=(i, item),
                    daemon=True)
                t.start()
                threads.append(t)
                # 每启动一个线程后小等一下，避免同时发太多请求
                time.sleep(0.1)

            for t in threads:
                t.join()

            if not cancel_ev.is_set():
                try:
                    on_done(enriched_count[0])
                except Exception:
                    pass

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()


# ── 集成到搜索流程的补全调用示例（在 Dashboard._on_preview 里使用）─

def _apply_enrichment(table, items_ref, log, auto_flush, index,
                      enricher, cookie, enrich_enabled):
    """
    搜索完成后调用此函数启动补全。
    items_ref: list，共享引用，补全时直接修改 items_ref[i]
    """
    if not enrich_enabled:
        return

    def _on_enriched(i, updated_item):
        # 更新 items_ref 中的数据
        if i < len(items_ref):
            items_ref[i] = updated_item
        # 更新预览表格对应行的 content widget
        if i < len(table._content_widgets) and i < len(table._items):
            table._items[i] = updated_item
            # 保留当前状态方块
            st, reason = (table._st_states[i]
                          if i < len(table._st_states) else (None, ''))
            table._content_widgets[i].value = \
                table._content_html(i, updated_item, st, reason)
        auto_flush()

    def _on_done(n):
        if n > 0:
            log.write(f'补全完成：{n} 个视频的发布时间/频道名已更新')
        auto_flush()

    enricher.start(
        items      = list(items_ref),
        cookie_path= cookie,
        on_item_enriched = _on_enriched,
        on_done    = _on_done,
        enabled    = enrich_enabled)