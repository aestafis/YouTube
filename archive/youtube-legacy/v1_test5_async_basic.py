# ════════════════════════════════════════════════════════
# TEST 5 ── 基本 async 更新（直接在 cell 里写 await）
# 目的：确认 await asyncio.sleep() 能让 widget 实时更新
# 耗时：约 10 秒
# ════════════════════════════════════════════════════════
import ipywidgets as W, asyncio
from IPython.display import display

w5h = W.HTML('<b>等待...</b>')
w5p = W.IntProgress(value=0, min=0, max=10,
                    bar_style='success', layout=W.Layout(width='60%'))
w5l = W.Label('未开始')
display(W.VBox([w5h, w5p, w5l]))

# 顶层 await：Colab/IPython 原生支持
for i in range(10):
    await asyncio.sleep(1)          # cell 保持 running，同时让出给 IOLoop
    w5h.value = f'<b style="color:green">async 更新 {i+1}/10</b>'
    w5p.value = i + 1
    w5l.value = f'step {i+1} 完成'

w5h.value = '<b>TEST 5 结束 — 如果每秒都更新了，async 方案可行</b>'