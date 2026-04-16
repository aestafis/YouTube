# ══════════════════════════════════════════════════════════════
# BLOCK 4 ── 日志（Textarea，子线程赋值 .value，约 1s 可见）
# ══════════════════════════════════════════════════════════════
class LiveLog:
    _MAX_LINES = 200   # 超出后裁掉最旧的行，防止 value 字符串无限增长

    def __init__(self):
        self._w = W.Textarea(
            value='',
            disabled=True,          # 只读，不可手动编辑
            placeholder='下载日志将在此显示...',
            layout=W.Layout(width='100%', height='220px'))
        self._lock  = threading.Lock()
        self._lines = []

    def write(self, msg):
        ts   = datetime.now().strftime('%H:%M:%S')
        line = f'[{ts}] {msg}'
        with self._lock:
            self._lines.append(line)
            if len(self._lines) > self._MAX_LINES:
                self._lines = self._lines[-self._MAX_LINES:]
            # 整体赋值：Colab 对值 widget 的推送比 Output 流更可靠
            self._w.value = '\n'.join(self._lines)

    def clear(self):
        with self._lock:
            self._lines = []
            self._w.value = ''

    def widget(self): return self._w