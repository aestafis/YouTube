# ════════════════════════════════════════════════════════
# TEST 4 ── reset_cb 竞态 bug 复现 + run_id 修复验证
# 目的：模拟 下载→停止→再下载，验证旧线程的 reset_cb
#       是否会覆盖新下载的按钮状态
# ════════════════════════════════════════════════════════
import ipywidgets as W, threading, time
from IPython.display import display

btn    = W.Button(description='▶ 开始', button_style='success',
                  layout=W.Layout(width='110px'))
status = W.HTML(value='<b>待机</b>')
log4   = W.Textarea(value='', disabled=True,
                    layout=W.Layout(height='180px'))

display(W.VBox([W.HBox([btn, status]), log4,
                W.HTML('<hr><small>'
                       '步骤：① 点"开始" → ② 等约 1 秒 → ③ 再次点"开始"<br>'
                       '多重复几次，观察日志中 reset_cb 是否被守卫拦截'
                       '</small>')]))

_lines = []
def _log(msg):
    _lines.append(f'[{time.strftime("%H:%M:%S")}] {msg}')
    log4.value = '\n'.join(_lines[-15:])

_run_id  = [0]
_stop_ev = [threading.Event()]

def _on_start(_b=None):
    _stop_ev[0].set()              # 停止旧线程
    _run_id[0]  += 1
    my_id        = _run_id[0]
    _stop_ev[0]  = threading.Event()
    cur_stop     = _stop_ev[0]

    btn.disabled    = True
    btn.description = f'运行 #{my_id}'
    status.value    = f'<b style="color:orange">下载中 run#{my_id}</b>'
    _log(f'=== 第 {my_id} 次启动 ===')

    def _reset():
        if _run_id[0] == my_id:          # run_id 守卫
            btn.disabled    = False
            btn.description = '▶ 开始'
            status.value    = f'<b style="color:gray">已停止 run#{my_id}</b>'
            _log(f'run#{my_id} reset_cb 执行 ✓')
        else:
            _log(f'run#{my_id} reset_cb 被守卫拦截 '
                 f'(现在 run_id={_run_id[0]}) ✓')

    def _worker():
        for step in range(8):
            if cur_stop.is_set():
                _log(f'run#{my_id} step {step} 收到停止')
                break
            time.sleep(0.5)
            _log(f'run#{my_id} step {step+1}/8')
        time.sleep(0.3)              # 模拟文件复制收尾
        _log(f'run#{my_id} → 调用 reset_cb')
        _reset()

    threading.Thread(target=_worker, daemon=True).start()

btn.on_click(_on_start)
print('TEST 4 就绪')
print('操作：① 点"开始" → ② 1 秒后再点"开始" → ③ 重复 2-3 次')
print('观察：日志里旧线程的 reset_cb 是否显示"被守卫拦截"')