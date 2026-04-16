# ════════════════════════════════════════════════════════
# TEST 6 ── async heartbeat + 后台线程（完整方案验证）
# 目的：下载线程只写队列，async heartbeat 每 0.2s 读队列并更新 widget
#       这是实际下载代码会采用的架构
# 耗时：约 5 秒
# ════════════════════════════════════════════════════════
import ipywidgets as W, asyncio, threading, hashlib, queue, time
from IPython.display import display

w6h = W.HTML('<b>等待...</b>')
w6p = W.IntProgress(value=0, min=0, max=100,
                    bar_style='info', layout=W.Layout(width='60%'))
w6l = W.Label('未开始')
display(W.VBox([w6h, w6p, w6l,
                W.HTML('<small>下载线程只写队列，不碰 widget<br>'
                       'async heartbeat 读队列、更新 widget、await 让出</small>')]))

_q6 = queue.Queue()

def _dl_thread():
    """模拟 yt-dlp：重量级工作，只往队列里写，不碰任何 widget"""
    for i in range(100):
        hashlib.sha256(b'chunk' * 4000).hexdigest()   # CPU 工作
        time.sleep(0.02)                              # 网络等待
        _q6.put(('pct', i + 1))
    _q6.put(('done', None))

threading.Thread(target=_dl_thread, daemon=True).start()

# async heartbeat：cell 保持 running，每 0.2s 读队列 + 更新 widget
t0 = time.time()
while True:
    await asyncio.sleep(0.2)        # ← 让出，widget comm 消息被处理
    latest, done = None, False
    while not _q6.empty():
        try:
            k, v = _q6.get_nowait()
            if k == 'pct':  latest = v
            if k == 'done': done   = True
        except: break
    if latest:
        w6h.value = f'<b style="color:blue">TEST6 async+线程: {latest}%</b>'
        w6p.value = latest
        w6l.value = f'{latest}%  耗时 {time.time()-t0:.1f}s'
    if done:
        w6h.value = '<b style="color:green">TEST 6 完成！进度是否全程流畅？</b>'
        w6p.value = 100
        break