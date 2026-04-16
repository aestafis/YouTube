# ══════════════════════════════════════════════════════════════
# MODULE B — 暂停/继续/终止 逻辑修复
# 解决: A-10
# ══════════════════════════════════════════════════════════════

# 暂停设计说明（最终版）：
# - 暂停是"视频级"：当前视频下载完后才暂停
# - 点暂停：立即 disable 暂停按钮，enable 继续按钮
#           状态栏立即显示"当前视频下完后暂停"
#           不等下载线程响应就更新 UI（flush_queue 同步调用）
# - 点继续：立即 disable 继续按钮，enable 暂停按钮
#           clear pause_ev，下载线程立即感知
# - 点终止：立即 set stop_ev，disable 终止按钮
#           下载线程在 _Logger.debug 里检查 stop_ev，raise _StopDownload

def _on_pause(self, w_pause, w_resume):
    self._pause_ev.set()
    w_pause.disabled  = True
    w_resume.disabled = False
    # 立即推送，不等 auto_flush
    try:
        self._status._w.value = _sb_html(
            'pause', '||',
            f'当前视频下完后暂停 ({self._cur_idx}/{self._cur_total})')
    except Exception:
        pass
    self._log.write(
        f'[暂停] 当前视频({self._cur_idx}/{self._cur_total})'
        f'下完后停止，点继续恢复')
    self._flush_queue()

def _on_resume(self, w_pause, w_resume):
    self._pause_ev.clear()
    # 立即更新按钮，不等线程响应
    w_pause.disabled  = False
    w_resume.disabled = True
    try:
        self._status._w.value = _sb_html('dl', '>', '继续下载中...')
    except Exception:
        pass
    self._log.write('[继续] 恢复下载')
    self._flush_queue()

def _on_stop(self):
    self._stop_ev.set()
    self._pause_ev.clear()
    if 'stop' in self._w:
        self._w['stop'].disabled    = True
        self._w['stop'].description = 'Stopping'
    self._log.write('[终止] 当前分片下完后中断...')
    # 立即推送
    try:
        self._status._w.value = _sb_html('stop', '[]', '终止中...')
    except Exception:
        pass
    self._flush_queue()