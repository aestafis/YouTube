# ════════════════════════════════════════════════════════
# TEST 3 ── Flush Thread 方案（与 TEST 2 完全相同的任务）
# 目的：下载线程只写队列，专用刷新线程每 0.3s 取最新值更新 widget
#       sleep 期间 GIL 释放 → IOLoop 推送 comm 消息
# 耗时：约 5 秒
# ════════════════════════════════════════════════════════
import ipywidgets as W, threading, time, hashlib, queue
from IPython.display import display

w3_html = W.HTML(value='<b>等待...</b>')
w3_prog = W.IntProgress(value=0, min=0, max=100, bar_style='info',
                         description='进度:', layout=W.Layout(width='60%'))
w3_lbl  = W.Label(value='未开始')

display(W.VBox([w3_html, w3_prog, w3_lbl,
                W.HTML('<hr><small>'
                       '与 TEST 2 完全相同的任务，但通过 Flush Thread 更新<br>'
                       '对比两个测试的更新流畅度'
                       '</small>')]))

_q3 = queue.Queue()

def _flush_loop():
    """专用刷新线程：每 0.3s 取最新值推到 widget，sleep 让 IOLoop 有机会工作"""
    while True:
        time.sleep(0.3)                    # ← GIL 释放点
        latest = {}
        while True:
            try:   latest.update([_q3.get_nowait()])
            except queue.Empty: break
        if 'html' in latest:  w3_html.value = latest['html']
        if 'prog' in latest:  w3_prog.value = latest['prog']
        if 'lbl'  in latest:  w3_lbl.value  = latest['lbl']

threading.Thread(target=_flush_loop, daemon=True).start()   # 先启动刷新线程

def _busy_worker3():
    t0 = time.time()
    for i in range(100):
        hashlib.sha256(b'chunk' * 4000).hexdigest()
        time.sleep(0.03)
        pct = i + 1
        _q3.put(('html', f'<b style="color:blue">TEST3 Flush: {pct}%</b>'))
        _q3.put(('prog', pct))
        _q3.put(('lbl',  f'{pct}%  耗时 {time.time()-t0:.1f}s'))

    _q3.put(('html', '<b style="color:green">TEST 3 完成</b>'))
    _q3.put(('prog', 100))

threading.Thread(target=_busy_worker3, daemon=True).start()
print('TEST 3 运行中（约 5 秒）')
print('核心问题：与 TEST 2 相比，进度条是否更流畅、更实时？')