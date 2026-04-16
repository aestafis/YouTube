    def _on_pause(self, w_pause, w_resume, w_stop):
        self._pause_ev.set()
        w_pause.disabled  = True
        w_resume.disabled = False
        w_stop.disabled   = False
        # 从按钮描述解析当前进度，格式为 "下载N/M"
        desc = self._w['dl'].description
        try:
            cur, tot = map(int, desc.replace('下载','').split('/'))
        except Exception:
            cur, tot = 0, 0
        self._status.paused(cur, tot)
        self._log.write('已暂停 — 点"继续"恢复，点"终止"停止')