# ════════════════════════════════════════════════════════
# TEST 1 ── 基准线：后台线程轻量更新（1 秒间隔）
# 目的：确认 4 种 widget 在 轻量子线程 下是否能实时更新
# 耗时：约 10 秒
# ════════════════════════════════════════════════════════
import ipywidgets as W, threading, time
from IPython.display import display

w_html  = W.HTML(value='HTML:  等待...')
w_ta    = W.Textarea(value='Textarea: 等待...', disabled=True,
                     layout=W.Layout(height='55px'))
w_prog  = W.IntProgress(value=0, min=0, max=10, bar_style='info',
                         description='Progress:', layout=W.Layout(width='55%'))
w_lbl   = W.Label(value='Label: 等待...')

display(W.VBox([w_html, w_ta, w_prog, w_lbl,
                W.HTML('<hr><small>观察：4 个 widget 是否每 1 秒递增一次</small>')]))

def _worker():
    for i in range(1, 11):
        time.sleep(1)
        ts = time.strftime('%H:%M:%S')
        w_html.value  = f'<b style="color:green">HTML: {i}/10 @ {ts}</b>'
        w_ta.value    = f'Textarea: {i}/10 @ {ts}'
        w_prog.value  = i
        w_lbl.value   = f'Label: {i}/10 @ {ts}'
    w_html.value = '<b style="color:gray">TEST 1 结束</b>'

threading.Thread(target=_worker, daemon=True).start()
print('TEST 1 运行中（10 秒）')
print('请记录：哪些 widget 实时更新 / 哪些没动 / 哪些有延迟')