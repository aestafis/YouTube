# ══════════════════════════════════════════════════════════════
# BLOCK 3 ── 状态栏
# ══════════════════════════════════════════════════════════════
class StatusBar:
    _TMPL = (
        '<div style="font-size:12px;font-family:monospace;'
        'background:{bg};color:{fg};padding:6px 12px;'
        'border-radius:4px;border:1px solid {border};'
        'line-height:1.8;margin:2px 0;'
        'white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'
        '{icon}&nbsp;{msg}'
        '</div>')

    def __init__(self):
        self._w = W.HTML(
            value=self._render('⬜','#f5f5f5','#888','#ddd','待机中'),
            layout=W.Layout(width='100%'))

    def _render(self, state, bg, fg, border, msg):
        return self._TMPL.format(bg=bg,fg=fg,border=border,
                                 icon=state,msg=msg)

    def idle(self):
        self._w.value = self._render(
            '⬜','#f5f5f5','#888','#ddd','待机中')

    def searching(self, query):
        self._w.value = self._render(
            '🔍','#e3f2fd','#1565c0','#90caf9',
            f'正在搜索: {query}')

    def found(self, n, mode):
        self._w.value = self._render(
            '✅','#e8f5e9','#2e7d32','#a5d6a7',
            f'找到 {n} 个视频（{mode}），勾选后点"开始下载"')

    def downloading(self, idx, total, title):
        """进度 0%，后续由 update_progress 驱动小鱼前进"""
        w = 16
        self._w.value = self._render(
            '⬇','#fff8e1','#e65100','#ffcc02',
            f'<span style="font-family:monospace">'
            f'|{"." * w}|</span>'
            f'&nbsp;--&nbsp;{idx}/{total}'
            f'&nbsp;|&nbsp;{_trim(title, 28)}')

    def update_progress(self, pct, idx, n, title):
        """
        小鱼进度条，整体替换 .value（不追加行）。
        子线程每 10% 调用一次，频率低，Colab HTML widget 可靠推送。
        """
        w   = 16
        pos = min(int(w * pct / 100), w - 1)
        if pct >= 99.5:
            bar  = '▓' * w
            fish = f'|{bar}|&nbsp;🐠'
        else:
            left  = '~' * pos
            right = '-' * (w - pos - 1)
            fish  = f'|{left}🐟{right}|'
        pct_s = f'{int(pct):3d}%'
        self._w.value = self._render(
            '⬇','#fff8e1','#e65100','#ffcc02',
            f'<span style="font-family:monospace">{fish}</span>'
            f'&nbsp;{pct_s}&nbsp;{idx}/{n}'
            f'&nbsp;|&nbsp;{_trim(title, 28)}')

    def paused(self, idx, total):
        self._w.value = self._render(
            '⏸','#fce4ec','#c62828','#ef9a9a',
            f'已暂停 ({idx}/{total})，点"继续"恢复')

    def done(self, done, fails, size, elapsed):
        self._w.value = self._render(
            '🎉','#e8f5e9','#2e7d32','#a5d6a7',
            f'下载完成&nbsp;&nbsp;✓{done}个'
            f'&nbsp;✗{fails}个'
            f'&nbsp;{_fmt_size(size)}'
            f'&nbsp;{elapsed:.0f}s')

    def stopped(self, done, fails):
        self._w.value = self._render(
            '⏹','#f5f5f5','#888','#ddd',
            f'已停止&nbsp;&nbsp;✓{done}个&nbsp;✗{fails}个')

    def error(self, msg):
        self._w.value = self._render(
            '❌','#fce4ec','#c62828','#ef9a9a', msg)

    def widget(self): return self._w