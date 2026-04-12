# ══════════════════════════════════════════════════════════════
# MODULE 1 — PreviewTable
# 解决: P0-1(勾选同步) / P0-2(拖拽同步) / P0-4(灰色方块)
# ══════════════════════════════════════════════════════════════

# ── JS: 拖拽视觉反馈 + pointerup 时回传 Python ────────────────
# 核心改变：
#   - pointermove 只做视觉（改 DOM checkbox.checked），不 dispatchEvent
#   - pointerup 时调 invokeFunction 把 (lo, hi, targetVal) 传给 Python
#   - Python 端收到后才真正改 cb.value + 更新 _selected_set
_DRAG_JS = """
<script>
(function(){
  if (window._yt_drag_v330) return;
  window._yt_drag_v330 = true;

  var D = {on:false, startIdx:-1, curIdx:-1, targetVal:null};

  function _allCbs(){
    // 只选 .yt-preview-cb 类，避免选中全选框等无关checkbox
    return Array.from(document.querySelectorAll(
      '.yt-preview-cb input[type=checkbox]'));
  }
  function _getCb(el){
    if(!el) return null;
    // 向上找最近的 .yt-preview-cb 容器
    var w = el.closest ? el.closest('.yt-preview-cb') : null;
    if(w) return w.querySelector('input[type=checkbox]');
    if(el.type==='checkbox') return el;
    return null;
  }

  document.addEventListener('pointerdown', function(e){
    var cb = _getCb(e.target);
    if(!cb) return;
    var cbs = _allCbs();
    var idx = cbs.indexOf(cb);
    if(idx < 0) return;
    // targetVal: 当前是勾选则拖拽取消，当前是取消则拖拽勾选
    D.on        = true;
    D.startIdx  = idx;
    D.curIdx    = idx;
    D.targetVal = !cb.checked;
    // 视觉上立即改当前格
    cb.checked  = D.targetVal;
    e.preventDefault();
  }, {capture:true, passive:false});

  document.addEventListener('pointermove', function(e){
    if(!D.on) return;
    var el  = document.elementFromPoint(e.clientX, e.clientY);
    var cb2 = _getCb(el);
    if(!cb2) return;
    var cbs = _allCbs();
    var idx = cbs.indexOf(cb2);
    if(idx < 0) return;
    D.curIdx = idx;
    var lo = Math.min(D.startIdx, idx);
    var hi = Math.max(D.startIdx, idx);
    // 只做视觉，不 dispatchEvent（避免触发不可靠的 comm）
    cbs.forEach(function(c, i){
      if(i >= lo && i <= hi) c.checked = D.targetVal;
      // 范围外不变
    });
  }, {capture:true, passive:true});

  document.addEventListener('pointerup', function(){
    if(!D.on){ D.on=false; return; }
    D.on = false;
    var lo  = Math.min(D.startIdx, D.curIdx);
    var hi  = Math.max(D.startIdx, D.curIdx);
    var val = D.targetVal ? 1 : 0;
    // ★ 关键：通过 Colab kernel 把拖拽结果回传 Python
    try{
      google.colab.kernel.invokeFunction(
        '_yt_drag_commit', [lo, hi, val], {});
    } catch(e) {
      // 非 Colab 环境（本地测试）：什么都不做，视觉已更新
    }
  }, true);

  document.addEventListener('pointercancel', function(){
    D.on = false;
  }, true);
})();
</script>
"""

_AUTO_REFRESH_JS = """
<script>
(function(){
  if(window._yt_auto_timer) clearInterval(window._yt_auto_timer);
  window._yt_auto_timer = setInterval(function(){
    try{ google.colab.kernel.invokeFunction('_yt_dl_flush',[],{}); }
    catch(e){}
  }, 2000);
})();
</script>
"""

# 状态方块配置（纯 ASCII 图标，无 emoji）
_ST_CFG = {
    'downloading': ('#ff9800', '>'),
    'done':        ('#4caf50', '+'),
    'fail':        ('#f44336', 'x'),
    'skip':        ('#9e9e9e', '-'),
    'saved':       ('#2196f3', 'v'),
}

def _st_span(st=None, reason=''):
    """
    ★ P0-4 修复: st=None 时返回空字符串，不渲染任何元素
    避免深色主题下出现灰色占位方块
    """
    if not st:
        # 返回透明占位，保持 grid 对齐，但无可见内容
        return ('<span style="display:inline-block;'
                'width:20px;height:20px"></span>')
    color, icon = _ST_CFG.get(st, ('#bdbdbd','?'))
    tip = f' title="{reason}"' if reason else ''
    return (
        f'<span style="display:inline-block;width:20px;height:20px;'
        f'line-height:20px;text-align:center;border-radius:3px;'
        f'background:{color};color:#fff;font-size:11px;font-weight:bold;'
        f'cursor:default"{tip}>{icon}</span>')


class PreviewTable:
    """
    ★ P0-1 修复: _selected_set 方案
      - render() 时每个 checkbox 绑定 observe('value', _on_cb_change)
      - _on_cb_change 同步写入 self._selected_set
      - get_selected() 从 self._selected_set 读，不依赖 cb.value 实时性

    ★ P0-2 修复: JS 拖拽 -> Colab kernel callback -> _drag_commit()
      - JS 只做视觉反馈
      - pointerup 时调 _yt_drag_commit(lo, hi, val)
      - Python 端批量 setattr(cb, 'value', ...) + 更新 _selected_set

    ★ P0-4 修复: st=None 时不渲染灰色方块
    """

    def __init__(self):
        self._items           = []      # 当前预览的视频列表
        self._boxes           = []      # W.Checkbox 列表，顺序与 _items 严格对应
        self._content_widgets = []      # W.HTML 列表，对应每行的内容区域
        self._st_states       = []      # (st, reason) per row

        # ★ P0-1 核心: Python 端维护的选中状态集合（存 index）
        self._selected_set    = set()
        self._cb_lock         = threading.Lock()

        # 下载中行状态更新队列（线程安全）
        self._pending         = {}
        self._pending_lock    = threading.Lock()

        self._saved_ids       = set()
        self._is_downloading  = False

        self.container = W.VBox(layout=W.Layout(width='100%'))

    # ── 工具：_trim / _fmt_views / _fmt_age 在外部定义，这里直接调用 ──

    def set_saved_ids(self, ids):
        self._saved_ids = set(ids)

    def set_downloading(self, v):
        self._is_downloading = v

    # ── 行内容 HTML（含状态方块）────────────────────────────────
    def _content_html(self, i, r, st=None, reason=''):
        title = r.get('title', '')
        ts    = _trim(title, 32)
        ch    = _trim(r.get('channel') or 'N/A', 22)
        dur   = r.get('duration', 'N/A')
        url   = r.get('url', '#')
        views = _fmt_views(r.get('view_count'))
        age   = _fmt_age(r.get('upload_date', ''))
        bg    = '#fff' if i % 2 == 0 else '#f9f9f9'

        vh = (f'<span style="font-size:12px;color:#444">{views}</span>'
              if views else
              '<span style="color:#ccc;font-size:11px">-</span>')
        ah = (f'<span style="font-size:11px;color:#666">{age}</span>'
              if age else
              '<span style="color:#ccc;font-size:11px">-</span>')
        st_s = _st_span(st, reason)

        # grid: 序号(24) | 标题/频道(1fr) | 状态方块(24) | 播放量(70) | 发布时间(60) | 时长(52)
        return (
            f'<div style="display:grid;'
            f'grid-template-columns:24px 1fr 24px 70px 60px 52px;'
            f'gap:0 4px;align-items:center;min-height:54px;'
            f'padding:3px 4px;background:{bg};'
            f'border-bottom:1px solid #eee">'
            f'<div style="text-align:center;color:#aaa;font-size:11px">{i+1}</div>'
            f'<div style="min-width:0;overflow:hidden">'
            f'<a href="{url}" target="_blank" '
            f'style="color:#1a73e8;text-decoration:none;font-size:13px;'
            f'font-weight:500;display:block;line-height:1.5;'
            f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis"'
            f' title="{title}">{ts}</a>'
            f'<div style="color:#888;font-size:11px;white-space:nowrap;'
            f'overflow:hidden;text-overflow:ellipsis">{ch}</div>'
            f'</div>'
            f'<div style="text-align:center">{st_s}</div>'
            f'<div style="text-align:right;padding-right:4px">{vh}</div>'
            f'<div style="text-align:center">{ah}</div>'
            f'<div style="text-align:center;font-size:12px;color:#444">{dur}</div>'
            f'</div>'
        )

    # ── 渲染（重建所有行）────────────────────────────────────────
    def render(self, items):
        """
        下载中不允许重建，避免 _boxes 列表被替换导致勾选状态丢失
        """
        if self._is_downloading:
            return

        # 清空旧状态
        self._items           = list(items)
        self._boxes           = []
        self._content_widgets = []
        self._st_states       = []
        with self._cb_lock:
            self._selected_set.clear()
        with self._pending_lock:
            self._pending.clear()

        if not items:
            self.container.children = (
                W.HTML(
                    '<div style="padding:20px;text-align:center;'
                    'color:#999;font-size:13px">未找到结果</div>'),)
            return

        rows = []
        for i, r in enumerate(items):
            is_saved   = bool(r.get('id') and r['id'] in self._saved_ids)
            init_val   = not is_saved   # 已存默认不勾选
            init_st    = 'saved' if is_saved else None

            # ★ P0-1: 创建 checkbox 并立即注册 observe
            cb = W.Checkbox(
                value=init_val,
                description='', indent=False,
                # ★ P0-2: 给行容器加 CSS class，供 JS 精确查找
                layout=W.Layout(
                    width='40px', min_width='40px',
                    height='54px', padding='0 4px'))

            # 把初始状态写入 _selected_set
            if init_val:
                with self._cb_lock:
                    self._selected_set.add(i)

            # ★ P0-1 核心绑定：observe 回调同步写 _selected_set
            def _on_change(change, _i=i):
                with self._cb_lock:
                    if change['new']:
                        self._selected_set.add(_i)
                    else:
                        self._selected_set.discard(_i)
            cb.observe(_on_change, names='value')

            self._boxes.append(cb)
            self._st_states.append((init_st, ''))

            cw = W.HTML(
                value=self._content_html(i, r, init_st, ''),
                layout=W.Layout(flex='1', min_width='0'))
            self._content_widgets.append(cw)

            # ★ HBox 严格只有 [cb, cw]
            # cb 加 CSS class 用于 JS 识别（通过 add_class）
            try:
                cb.add_class('yt-preview-cb')
            except Exception:
                pass

            rows.append(W.HBox(
                [cb, cw],
                layout=W.Layout(
                    width='100%', align_items='center',
                    min_height='54px')))

        # ── 控制区 ──────────────────────────────────────────────
        # 全选
        all_cb = W.Checkbox(
            value=True, description='全选', indent=False,
            layout=W.Layout(width='64px', min_width='64px'))
        def _toggle_all(c):
            val = c['new']
            # 批量更新 checkbox（会触发各自的 observe 回调）
            for b in self._boxes:
                b.value = val
        all_cb.observe(_toggle_all, names='value')

        # 勾选已存 / 取消已存
        btn_sel_saved = W.Button(
            description='勾选已存',
            layout=W.Layout(width='76px', height='26px'),
            style={'font_size':'11px', 'button_color':'#bbdefb'},
            tooltip='将预览中所有已存视频全部勾选')
        btn_unsel_saved = W.Button(
            description='取消已存',
            layout=W.Layout(width='76px', height='26px'),
            style={'font_size':'11px', 'button_color':'#ffccbc'},
            tooltip='将预览中所有已存视频全部取消勾选')

        def _sel_saved(_):
            for r, b in zip(self._items, self._boxes):
                if r.get('id','') in self._saved_ids:
                    b.value = True   # 触发 observe，自动更新 _selected_set
        def _unsel_saved(_):
            for r, b in zip(self._items, self._boxes):
                if r.get('id','') in self._saved_ids:
                    b.value = False

        btn_sel_saved.on_click(_sel_saved)
        btn_unsel_saved.on_click(_unsel_saved)

        # 表头
        header = W.HTML(
            '<div style="display:grid;'
            'grid-template-columns:24px 1fr 24px 70px 60px 52px;'
            'gap:0 4px;font-size:11px;color:#888;'
            'background:#f2f2f2;padding:5px 4px;'
            'border-bottom:2px solid #ccc">'
            '<div style="text-align:center">#</div>'
            '<div style="padding-left:4px">标题 / 频道</div>'
            '<div style="text-align:center" title="下载状态">态</div>'
            '<div style="text-align:right;padding-right:4px">播放量</div>'
            '<div style="text-align:center">发布时间</div>'
            '<div style="text-align:center">时长</div>'
            '</div>')

        # 范围选择（100% Python，不依赖 JS）
        wf = W.BoundedIntText(
            value=1, min=1, max=len(items), step=1,
            description='从', style={'description_width':'20px'},
            layout=W.Layout(width='74px'))
        wt = W.BoundedIntText(
            value=len(items), min=1, max=len(items), step=1,
            description='到', style={'description_width':'20px'},
            layout=W.Layout(width='74px'))
        ws = W.Button(
            description='勾选',
            layout=W.Layout(width='52px', height='26px'),
            style={'font_size':'11px', 'button_color':'#e8f5e9'})
        wd = W.Button(
            description='取消',
            layout=W.Layout(width='52px', height='26px'),
            style={'font_size':'11px', 'button_color':'#fce4ec'})

        # ★ 范围选择：直接 setattr cb.value，触发 observe，可靠
        ws.on_click(lambda _: [
            setattr(b, 'value', True)
            for b in self._boxes[wf.value-1:wt.value]])
        wd.on_click(lambda _: [
            setattr(b, 'value', False)
            for b in self._boxes[wf.value-1:wt.value]])

        has_saved = bool(self._saved_ids & {r.get('id','') for r in items})
        note = W.HTML(
            f'<div style="font-size:10px;color:#888;padding:4px 8px;'
            f'background:#fafafa;border-bottom:1px solid #eee">'
            f'共 <b>{len(items)}</b> 个'
            f'{"  按播放量排序" if len(items) > 1 else ""}'
            f'{"  [v]蓝=已存(默认不勾选)" if has_saved else ""}'
            f'  [>]橙=下载中  [+]绿=完成  [x]红=失败'
            f'  |  拖拽或用范围按钮批量选'
            f'</div>')

        ctrl = W.HBox(
            [all_cb,
             W.HTML('<span style="font-size:10px;color:#888;'
                    'margin:auto 4px">已存:</span>'),
             btn_sel_saved, btn_unsel_saved],
            layout=W.Layout(align_items='center', margin='2px 0'))

        range_row = W.HBox(
            [W.HTML('<span style="font-size:11px;color:#888;'
                    'margin:auto 4px">范围:</span>'),
             wf, wt, ws, wd,
             W.HTML('<span style="font-size:10px;color:#aaa;'
                    'margin:auto 6px">或拖拽复选框批量选</span>')],
            layout=W.Layout(align_items='center', margin='3px 0'))

        self.container.children = tuple(
            [note, ctrl, range_row, header] + rows)

    # ── 拖拽回调（由 JS pointerup 触发）─────────────────────────
    def drag_commit(self, lo, hi, target_val):
        """
        ★ P0-2: JS 拖拽完成后，Python 端批量更新
        lo, hi: 拖拽范围（0-indexed，闭区间）
        target_val: True=勾选, False=取消
        """
        lo = max(0, int(lo))
        hi = min(len(self._boxes)-1, int(hi))
        for i in range(lo, hi+1):
            if i < len(self._boxes):
                # setattr 会触发 observe 回调，自动更新 _selected_set
                self._boxes[i].value = bool(target_val)

    # ── 获取选中列表（★ P0-1: 从 _selected_set 读）────────────
    def get_selected(self):
        """
        返回值语义：
          None  → 尚无预览结果（_items 为空）
          []    → 有预览但全部取消勾选
          [...] → 正常选中列表
        从 _selected_set 读取，不依赖 cb.value 的异步性
        """
        if not self._items:
            return None
        with self._cb_lock:
            selected_indices = sorted(self._selected_set)
        return [self._items[i] for i in selected_indices
                if i < len(self._items)]

    # ── 状态方块更新（线程安全）─────────────────────────────────
    def mark(self, vid_id, status, reason=''):
        """下载线程调用：入队行状态更新"""
        if not vid_id:
            return
        with self._pending_lock:
            self._pending[vid_id] = (status, reason)

    def apply_pending_marks(self):
        """主线程（flush_queue）调用：批量应用状态更新"""
        with self._pending_lock:
            marks = dict(self._pending)
            self._pending.clear()
        if not marks:
            return
        for vid_id, (st, reason) in marks.items():
            for i, item in enumerate(self._items):
                if item.get('id','') == vid_id:
                    if i < len(self._content_widgets):
                        self._st_states[i] = (st, reason)
                        self._content_widgets[i].value = \
                            self._content_html(i, item, st, reason)
                    break

    # ── 清空 ────────────────────────────────────────────────────
    def clear(self):
        self._items           = []
        self._boxes           = []
        self._content_widgets = []
        self._st_states       = []
        self._saved_ids       = set()
        self._is_downloading  = False
        with self._cb_lock:
            self._selected_set.clear()
        with self._pending_lock:
            self._pending.clear()
        self.container.children = ()