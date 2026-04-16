# ══════════════════════════════════════════════════════════════
# MODULE 4 — 下载逻辑
# 解决: P1-3(下载顺序) / drag_commit注册 / _guarded_reset恢复
# ══════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════
# MODULE 5 — Drive / State 延迟加载（验证保留 + get_dl_set）
# 解决: P0-5 验证
# ══════════════════════════════════════════════════════════════

# ★ 以下是完整的 Dashboard._on_download / _on_pause /
#   _on_resume / _on_stop / _register_callbacks 的精确实现
#   直接粘贴替换 Dashboard 里对应方法即可

class _DownloadMethods:
    """
    这不是独立类，是 Dashboard 里相关方法的聚合文档。
    最终组装时这些方法归属于 Dashboard。
    单独列出便于审查每个方法的正确性。
    """

    # ── Colab 回调注册（launch 时调用）──────────────────────────
    def _register_callbacks(self):
        """
        注册所有需要从 JS 调用的 Python 回调：
          _yt_dl_flush     : 2s 定时器触发，刷新 UI
          _yt_drag_commit  : JS 拖拽结束时触发，同步勾选状态
        """
        if not _IN_COLAB:
            return
        try:
            from google.colab import output as _co
            # flush 回调
            _co.register_callback(
                '_yt_dl_flush',
                lambda: self._flush_queue())
            # ★ P0-2: drag_commit 回调
            # Colab invokeFunction 传过来的参数是 JSON 反序列化后的 Python 对象
            # lo/hi 是 float（JSON number），target 是 int(0 或 1)
            _co.register_callback(
                '_yt_drag_commit',
                lambda lo, hi, target: self._table.drag_commit(
                    int(lo), int(hi), bool(int(target))))
        except Exception:
            pass

    # ── 开始下载（★ 完整修复版）────────────────────────────────
    def _on_download(self, query, sort, count, cookie, save,
                     w_dl, w_pause, w_resume, w_stop):
        """
        修复清单：
        1. ★ P1-3: 删除 sorted(selected, by view_count)，保持勾选顺序
        2. ★ _guarded_reset: 恢复 run_id 检查，防止旧下载重置新下载的按钮
        3. ★ _last_results=None 在 flush 前设置，防止 render 重建 boxes
        4. ★ 线程外读取 skip_saved / subtitle_on，避免闭包延迟读
        """
        # ★ 先清 _last_results，防止 flush 触发 render 重建 boxes
        self._last_results = None
        self._flush_queue()
        # 此刻 _boxes 稳定，_selected_set 可靠

        # 线程外读取，避免闭包延迟
        subtitle_on  = self._w.get('subtitle') and self._w['subtitle'].value
        skip_saved   = self._w.get('skip_saved') and self._w['skip_saved'].value
        enrich_on    = self._w.get('enrich') and self._w['enrich'].value
        search_first = False
        itype, idata = _parse_input(query)

        # ── 确定下载列表 ─────────────────────────────────────────
        if itype in ('single_url', 'multi_url', 'channel', 'channel_multi_warn'):
            selected = self._table.get_selected()
            if selected:
                items = selected          # ★ P1-3: 不 sorted，保持勾选顺序
            elif itype == 'single_url':
                items = [{
                    'id': '', 'title': _trim(idata, 52), 'url': idata,
                    'channel': '', 'duration': 'N/A', 'dur_s': 0,
                    'view_count': None, 'upload_date': ''}]
            else:
                self._log.write('请先点"搜索预览"再下载')
                self._flush_queue()
                return
        else:
            selected = self._table.get_selected()
            if selected is None:
                # 从未预览过 → 先搜索再下载
                if not query:
                    self._log.write('请输入关键词或URL')
                    self._flush_queue()
                    return
                items = None
                search_first = True
            elif len(selected) == 0:
                self._log.write('请至少勾选一个视频再下载')
                self._status.error('请至少勾选一个视频')
                self._flush_queue()
                return
            else:
                items = list(selected)    # ★ P1-3: 不 sorted，保持勾选顺序

        # ── 按钮状态：进入下载模式 ─────────────────────────────
        w_dl.disabled    = True;  w_dl.description    = '下载中...'
        w_pause.disabled = False
        w_pause.description = '暂停'
        w_resume.disabled= True
        w_stop.disabled  = False; w_stop.description  = '终止'

        # ★ _guarded_reset: 记录本次 run_id
        self._run_id += 1
        my_run_id = self._run_id

        self._stop_ev  = threading.Event()
        self._pause_ev = threading.Event()
        stop_ev  = self._stop_ev
        pause_ev = self._pause_ev

        self._dl_running = True
        self._table.set_downloading(True)

        # ★ 取消正在进行的补全（下载开始时停止补全请求，节省带宽）
        if hasattr(self, '_enricher'):
            self._enricher.cancel()

        # ★ _guarded_reset: 只有 run_id 匹配时才重置按钮
        def _guarded_reset():
            if self._run_id == my_run_id:
                self._do_reset_dl_btns()

        def _prog(idx, n, _):
            self._cur_idx   = idx
            self._cur_total = n

        def _run():
            nonlocal items
            try:
                # Drive 挂载（在线程里，不阻塞主线程）
                ok, msg = _mount_drive()
                self._log.write(f'Drive: {msg}')
                self._auto_flush()
                if not ok:
                    self._status.error(f'Drive连接失败:{msg}')
                    self._auto_flush()
                    return

                # Cookie 检查
                try:
                    found = _check_cookie(cookie)
                    self._log.write(f'Cookie OK: {found}')
                    self._auto_flush()
                except CookieError as e:
                    self._log.write(f'Cookie失效:{e}')
                    self._status.error(f'Cookie失效:{e}')
                    self._auto_flush()
                    return

                # 先搜索模式
                if search_first:
                    self._log.write('未预览，先搜索...')
                    surl = _build_url(idata, sort)
                    if not surl:
                        self._log.write('无效输入')
                        return
                    try:
                        saved = (self._index.get_all_ids() |
                                 self._state.get_dl_set())
                        res, _sk = _do_search(
                            surl, count, cookie, self._mode_cfg,
                            saved_ids=saved if skip_saved else None,
                            skip_saved=bool(skip_saved))
                        if not res:
                            self._log.write('未找到视频')
                            self._auto_flush()
                            return
                        # 临时显示预览（下载中不允许重建，先放行一次）
                        self._table.set_saved_ids(saved)
                        self._table.set_downloading(False)
                        self._last_results = res[:]
                        self._auto_flush()
                        self._table.set_downloading(True)
                        items = res   # ★ P1-3: 不 sorted
                    except CookieError as e:
                        self._log.write(f'Cookie失效:{e}')
                        self._auto_flush()
                        return

                # 建立 session 目录
                sd = _make_session_dir(
                    save, self._mode_name, query[:20], len(items))
                self._log.write(f'下载 {len(items)} 个  目录: {sd}')
                self._auto_flush()

                # 核心下载
                done, fails, sw, done_ids, tb, elapsed = _do_download(
                    items, cookie, save,
                    stop_ev, pause_ev,
                    self._state, sd,
                    self._log, self._status,
                    _prog,
                    flush_cb=self._auto_flush,
                    subtitle_on=subtitle_on,
                    table_mark_cb=self._table.mark)

                # 收尾
                self._index.invalidate()
                sl = next((k for k, v in SORT_OPTS.items() if v == sort), sort)
                _write_index_txt(
                    sd, self._mode_name, query[:40], sl, items, done_ids)

                if sw == 'user_stop':
                    self._status.stopped(done, fails)
                else:
                    self._status.done(done, fails, tb, elapsed)

            except Exception:
                self._log.write('下载崩溃:')
                self._log.write(traceback.format_exc()[-600:])
                self._status.error('下载崩溃，见日志')
            finally:
                # ★ _guarded_reset: 通过 put_cb 在主线程执行重置
                self._uiq.put_cb(_guarded_reset)
                self._auto_flush()

        threading.Thread(target=_run, daemon=True).start()

    # ── 暂停（主线程按钮回调）────────────────────────────────────
    def _on_pause(self, w_pause, w_resume):
        """
        直接操作 Event，直接调 _flush_queue（主线程，安全）
        不调 _auto_flush（避免无谓的 io_loop 调度）
        """
        self._pause_ev.set()
        w_pause.disabled  = True
        w_resume.disabled = False
        self._status.paused(self._cur_idx, self._cur_total)
        self._log.write('暂停请求已发送，当前视频下完后停止')
        self._flush_queue()

    # ── 继续（主线程按钮回调）────────────────────────────────────
    def _on_resume(self, w_pause, w_resume):
        self._pause_ev.clear()
        w_pause.disabled  = False
        w_resume.disabled = True
        self._status.resuming()
        self._log.write('继续下载...')
        self._flush_queue()

    # ── 终止（主线程按钮回调）────────────────────────────────────
    def _on_stop(self):
        self._stop_ev.set()
        self._pause_ev.clear()
        if 'stop' in self._w:
            self._w['stop'].description = '停止中...'
            self._w['stop'].disabled    = True
        self._log.write('正在中断...')
        self._flush_queue()

    # ── 重置下载按钮（主线程执行）────────────────────────────────
    def _do_reset_dl_btns(self):
        """
        由 _guarded_reset 通过 put_cb 在主线程调用
        确保只有最新的 run_id 才能执行重置
        """
        if 'dl' in self._w:
            self._w['dl'].disabled    = False
            self._w['dl'].description = '开始下载'
        if 'pause' in self._w:
            self._w['pause'].disabled    = True
            self._w['pause'].description = '暂停'
        if 'resume' in self._w:
            self._w['resume'].disabled = True
        if 'stop' in self._w:
            self._w['stop'].disabled    = True
            self._w['stop'].description = '终止'
        if 'prev' in self._w:
            self._w['prev'].disabled = False
        self._dl_running = False
        self._table.set_downloading(False)

    # ── 重启刷新（★ P2-1 按钮回调）──────────────────────────────
    def _on_refresh(self):
        self._flush_queue()
        if _IN_COLAB:
            try:
                display(HTML(_AUTO_REFRESH_JS))
            except Exception:
                pass
        self._log.write('自动刷新定时器已重启（2s 间隔）')
        self._flush_queue()

    # ── 搜索预览（含补全启动）────────────────────────────────────
    def _on_preview(self, query, sort, count, cookie, save,
                    w_prev, w_cancel):
        self._flush_queue()

        if not query:
            self._log.write('请输入关键词或URL')
            self._status.error('请输入关键词或URL')
            self._flush_queue()
            return

        try:
            _check_cookie(cookie)
        except CookieError as e:
            self._log.write(f'Cookie错误:{e}')
            self._status.error(f'Cookie:{e}')
            self._flush_queue()
            return

        itype, idata = _parse_input(query)

        # ★ 线程外读取（避免闭包延迟）
        skip_saved = (self._w['skip_saved'].value
                      if 'skip_saved' in self._w else True)
        enrich_on  = (self._w['enrich'].value
                      if 'enrich' in self._w else True)

        # 取消上次搜索和补全
        self._cancel_search_ev.set()
        if hasattr(self, '_enricher'):
            self._enricher.cancel()

        self._cancel_search_ev = threading.Event()
        cancel_ev = self._cancel_search_ev

        w_prev.disabled    = True
        w_prev.description = '搜索中...'
        w_cancel.disabled  = False
        self._flush_queue()

        def _search():
            res     = []
            mode    = self._mode_name
            skipped = 0
            try:
                # Drive 挂载 + 索引读取（在线程里）
                _mount_drive()
                saved = (self._index.get_all_ids() |
                         self._state.get_dl_set())
                self._log.write(
                    f'已存: {len(saved)} 个  '
                    f'{"跳过已存" if skip_saved else "不跳过"}')

                # 根据输入类型分派
                if itype == 'channel_multi_warn':
                    self._log.write(
                        f'检测到 {len(idata)} 个频道URL，只处理第一个')
                    actual = idata[0]
                    self._status.fetching_channel(actual)
                    res, cancelled = _fetch_channel(
                        actual, count, cookie, cancel_ev)
                    if cancelled:
                        self._status.cancelled()
                        return
                    mode = '频道'

                elif itype == 'keyword':
                    self._status.searching(idata)
                    self._log.write(f'关键词搜索: {idata}')
                    url = _build_url(idata, sort)
                    res, skipped = _do_search(
                        url, count, cookie, self._mode_cfg, cancel_ev,
                        saved_ids=saved if skip_saved else None,
                        skip_saved=skip_saved)
                    mode = self._mode_name

                elif itype == 'single_url':
                    self._status.searching(idata)
                    self._log.write(f'获取视频信息: {_trim(idata, 60)}')
                    item = _fetch_url_info(idata, cookie, cancel_ev)
                    res  = [item] if item else []
                    mode = 'URL'

                elif itype == 'multi_url':
                    self._status.fetching_urls(len(idata))
                    self._log.write(f'批量获取 {len(idata)} 个视频...')
                    def _prog(d, t):
                        self._log.write(f'  已获取 {d}/{t}')
                        self._auto_flush()
                    res  = _fetch_multi_urls(idata, cookie, cancel_ev, _prog)
                    mode = f'{len(res)}个URL'

                elif itype == 'channel':
                    self._status.fetching_channel(idata)
                    self._log.write(f'频道抓取: {_trim(idata, 60)}')
                    res, cancelled = _fetch_channel(
                        idata, count, cookie, cancel_ev)
                    if cancelled:
                        self._status.cancelled()
                        return
                    mode = '频道'

                if cancel_ev.is_set():
                    self._status.cancelled()
                    return

                # 更新预览表格
                self._table.set_saved_ids(saved)
                self._last_results = res   # flush 时触发 render

                if res:
                    self._log.write(
                        f'找到 {len(res)} 个 ({mode})'
                        + (f'  已跳过 {skipped} 个已存' if skipped else ''))
                    self._status.found(len(res), mode, skipped)
                    self._auto_flush()   # 先让预览显示出来

                    # ★ 模块3: 启动补全（异步，不阻塞预览显示）
                    if enrich_on and res:
                        self._log.write('补全发布时间/频道名中...')
                        self._auto_flush()
                        _apply_enrichment(
                            table        = self._table,
                            items_ref    = self._last_results
                                           if self._last_results else res,
                            log          = self._log,
                            auto_flush   = self._auto_flush,
                            index        = self._index,
                            enricher     = self._enricher,
                            cookie       = cookie,
                            enrich_enabled = enrich_on)
                else:
                    self._log.write(
                        '未找到符合条件的视频'
                        + (f' (已跳过{skipped}个)' if skipped else ''))
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

    # ── 取消搜索 ────────────────────────────────────────────────
    def _on_cancel_preview(self, w_cancel):
        self._cancel_search_ev.set()
        if hasattr(self, '_enricher'):
            self._enricher.cancel()
        w_cancel.disabled = True
        self._log.write('正在取消搜索...')
        self._flush_queue()

    # ── 重置记录 ────────────────────────────────────────────────
    def _on_reset(self, clear_index=False):
        self._state.reset(clear_index=clear_index)
        self._table.clear()
        self._last_results = None
        msg = '下载记录已清除' + ('（含已存索引）' if clear_index else '')
        self._log.write(msg)
        self._status.idle()
        self._flush_queue()