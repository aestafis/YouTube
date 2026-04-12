# ════════════════════════════════════════════════════════
# TEST 2 ── 繁忙线程（直接更新，模拟 yt-dlp 下载行为）
# 目的：CPU密集+网络等待 期间，直接调用 widget.value 是否实时可见
# 耗时：约 5 秒
# ════════════════════════════════════════════════════════
import ipywidgets as W, threading, time, hashlib
from IPython.display import display

w2_html = W.HTML(value='<b>等待...</b>')
w2_prog = W.IntProgress(value=0, min=0, max=100, bar_style='warning',
                         description='进度:', layout=W.Layout(width='60%'))
w2_lbl  = W.Label(value='未开始')

display(W.VBox([w2_html, w2_prog, w2_lbl,
                W.HTML('<hr><small>'
                       '观察：进度是 1%→2%→... 逐步爬升，'
                       '还是任务结束时一次性跳到 100%？'
                       '</small>')]))

def _busy_worker():
    t0 = time.time()
    for i in range(100):
        # 模拟 yt-dlp：处理数据块（CPU）
        hashlib.sha256(b'chunk' * 4000).hexdigest()
        # 模拟网络等待（GIL 释放）
        time.sleep(0.03)
        # 模拟 _Logger.debug 回调：直接更新 widget
        pct = i + 1
        w2_html.value = (f'<b style="color:orange">'
                         f'TEST2 繁忙直接更新: {pct}%</b>')
        w2_prog.value = pct
        w2_lbl.value  = f'{pct}%  耗时 {time.time()-t0:.1f}s'

    w2_html.value = '<b style="color:green">TEST 2 完成</b>'
    w2_prog.value = 100

threading.Thread(target=_busy_worker, daemon=True).start()
print('TEST 2 运行中（约 5 秒）')
print('核心问题：进度条是逐步爬升 还是 最后才出现？')