# ══════════════════════════════════════════════════════════════
# MODULE 1 REVISED — PreviewTable
# 修复: 全选框索引偏移 / observe未解绑 / 灰色方块
# ══════════════════════════════════════════════════════════════

_DRAG_JS = """
<script>
(function(){
  if (window._yt_drag_v330) return;
  window._yt_drag_v330 = true;

  var D = {on:false, startIdx:-1, curIdx:-1, targetVal:null};

  function _rowCbs(){
    // ★ 只扫描 .yt-rows-box 容器内的 checkbox
    // 完全隔离全选框、范围按钮等控制 checkbox
    var container = document.querySelector('.yt-rows-box');
    if(!container) return [];
    return Array.from(
      container.querySelectorAll('input[type=checkbox]'));
  }

  function _getCbInRows(el){
    if(!el) return null;
    var container = document.querySelector('.yt-rows-box');
    if(!container || !container.contains(el)) return null;
    if(el.type==='checkbox') return el;
    if(el.closest){
      var label = el.closest('label');
      if(label){
        var cb = label.querySelector('input[type=checkbox]');
        if(cb && container.contains(cb)) return cb;
      }
    }
    return null;
  }

  document.addEventListener('pointerdown', function(e){
    var cb = _getCbInRows(e.target);
    if(!cb) return;
    var cbs = _rowCbs();
    var idx = cbs.indexOf(cb);
    if(idx < 0) return;
    D.on        = true;
    D.startIdx  = idx;
    D.curIdx    = idx;
    D.targetVal = !cb.checked;
    cb.checked  = D.targetVal;
    e.preventDefault();
  }, {capture:true, passive:false});

  document.addEventListener('pointermove', function(e){
    if(!D.on) return;
    var el  = document.elementFromPoint(e.clientX, e.clientY);
    var cb2 = _getCbInRows(el);
    if(!cb2) return;
    var cbs = _rowCbs();
    var idx = cbs.indexOf(cb2);
    if(idx < 0) return;
    D.curIdx = idx;
    var lo = Math.min(D.startIdx, idx);
    var hi = Math.max(D.startIdx, idx);
    cbs.forEach(function(c, i){
      if(i >= lo && i <= hi) c.checked = D.targetVal;
    });
  }, {capture:true, passive:true});

  document.addEventListener('pointerup', function(){
    if(!D.on){ D.on=false; return; }
    D.on = false;
    var lo  = Math.min(D.startIdx, D.curIdx);
    var hi  = Math.max(D.startIdx, D.curIdx);
    var val = D.targetVal ? 1 : 0;
    try{
      google.colab.kernel.invokeFunction(
        '_yt_drag_commit', [lo, hi, val], {});
    }catch(e){}
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

_ST_CFG = {
    'downloading': ('#ff9800', '>'),
    'done':        ('#4caf50', '+'),
    'fail':        ('#f44336', 'x'),
    'skip':        ('#9e9e9e', '-'),
    'saved':       ('#2196f3', 'v'),
}

def _st_span(st=None, reason=''):
    # ★ st=None 返回透明占位，无可见内容，无背景色
    if not st:
        return ('<span style="display:inline-block;'
                'width:20px;height:20px"></span>')
    color, icon = _ST_CFG.get(st, ('#bdbdbd', '?'))
    tip = f' title="{reason}"' if reason else ''
    return (
        f'<span style="display:inline-block;width:20px;height:20px;'
        f'line-height:20px;text-align:center;border-radius:3px;'
        f'background:{color};color:#fff;font-size:11px;font-weight:bold;'
        f'cursor:default"{tip}>{icon}</span>')


class PreviewTable:

    def __init__(self):
        self._items               = []
        self._boxes               = []
        self._content_widgets     = []
        self._st_states           = []
        self._selected_set        = set()
        self._cb_lock             = threading.Lock()
        self._pending_marks       = {}   # vid_id -> (st, reason)
        self._pending_enrichments = {}   # index -> updated_item
        self._pending_lock        = threading.Lock()
        self._saved_ids           = set()
        self._is_downloading      = False
        # ★ 预览行单独容器，JS 通过 .yt-rows-box 类名精确查找
        self._rows_box  = W.VBox(layout=W.Layout(width='100%'))
        self.container  = W.VBox(layout=W.Layout(width='100%'))

    def set_saved_ids(self, ids):
        self._saved_ids = set(ids)

    def set_downloading(self, v):
        self._is_downloading = v

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
        return (
            f'<div style="display:grid;'
            f'grid-template-columns:24px 1fr 24px 70px 60px 52px;'
            f'gap:0 4px;align-items:center;min-height:54px;'
            f'padding:3px 4px;background:{bg};'
            f'border-bottom:1px solid #eee">'
            f'<div style="text-align:center;color:#aaa;font-size:11px">'
            f'{i+1}</div>'
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
            f'<div style="text-align:center;font-size:12px;color:#444">'
            f'{dur}</div>'
            f'</div>')

    def render(self, items):
        if self._is_downloading:
            return

        # ★ 解绑旧 checkbox 的 observe，防止幽灵回调
        for old_cb in self._boxes:
            try:
                old_cb.unobserve_all()
            except Exception:
                pass

        self._items               = list(items)
        self._boxes               = []
        self._content_widgets     = []
        self._st_states           = []
        with self._cb_lock:
            self._selected_set.clear()
        with self._pending_lock:
            self._pending_marks.clear()
            self._pending_enrichments.clear()

        if not items:
            self._rows_box.children = ()
            self.container.children = (
                W.HTML(
                    '<div style="padding:20px;text-align:center;'
                    'color:#999;font-size:13px">未找到结果</div>'),)
            return

        rows = []
        for i, r in enumerate(items):
            is_saved = bool(r.get('id') and r['id'] in self._saved_ids)
            init_val = not is_saved
            init_st  = 'saved' if is_saved else None

            cb = W.Checkbox(
                value=init_val,
                description='', indent=False,
                layout=W.Layout(
                    width='40px', min_width='40px',
                    height='54px', padding='0 4px'))

            if init_val:
                with self._cb_lock:
                    self._selected_set.add(i)

            # ★ observe 绑定，默认参数避免闭包陷阱
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

            rows.append(W.HBox(
                [cb, cw],
                layout=W.Layout(
                    width='100%', align_items='center',
                    min_height='54px')))

        # ★ 预览行放入专用容器，JS 靠 CSS class 精确定位
        self._rows_box.children = tuple(rows)
        try:
            self._rows_box.add_class('yt-rows-box')
        except Exception:
            pass

        # ── 控制区（全选、已存按钮、范围按钮）─────────────────────
        # ★ 控制区的 checkbox 不在 _rows_box 里，JS 扫描不到
        all_cb = W.Checkbox(
            value=True, description='全选', indent=False,
            layout=W.Layout(width='64px', min_width='64px'))

        def _toggle_all(c):
            val = c['new']
            for b in self._boxes:
                b.value = val   # 触发各自 observe，自动更新 _selected_set
        all_cb.observe(_toggle_all, names='value')

        btn_sel = W.Button(
            description='勾选已存',
            layout=W.Layout(width='76px', height='26px'),
            style={'font_size': '11px', 'button_color': '#bbdefb'},
            tooltip='将所有已存视频全部勾选')
        btn_unsel = W.Button(
            description='取消已存',
            layout=W.Layout(width='76px', height='26px'),
            style={'font_size': '11px', 'button_color': '#ffccbc'},
            tooltip='将所有已存视频全部取消勾选')

        def _sel_saved(_):
            for r, b in zip(self._items, self._boxes):
                if r.get('id', '') in self._saved_ids:
                    b.value = True
        def _unsel_saved(_):
            for r, b in zip(self._items, self._boxes):
                if r.get('id', '') in self._saved_ids:
                    b.value = False

        btn_sel.on_click(_sel_saved)
        btn_unsel.on_click(_unsel_saved)

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

        wf = W.BoundedIntText(
            value=1, min=1, max=len(items), step=1,
            description='从', style={'description_width': '20px'},
            layout=W.Layout(width='74px'))
        wt = W.BoundedIntText(
            value=len(items), min=1, max=len(items), step=1,
            description='到', style={'description_width': '20px'},
            layout=W.Layout(width='74px'))
        ws = W.Button(
            description='勾选',
            layout=W.Layout(width='52px', height='26px'),
            style={'font_size': '11px', 'button_color': '#e8f5e9'})
        wd = W.Button(
            description='取消',
            layout=W.Layout(width='52px', height='26px'),
            style={'font_size': '11px', 'button_color': '#fce4ec'})
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
             btn_sel, btn_unsel],
            layout=W.Layout(align_items='center', margin='2px 0'))

        range_row = W.HBox(
            [W.HTML('<span style="font-size:11px;color:#888;'
                    'margin:auto 4px">范围:</span>'),
             wf, wt, ws, wd,
             W.HTML('<span style="font-size:10px;color:#aaa;'
                    'margin:auto 6px">或拖拽复选框批量选</span>')],
            layout=W.Layout(align_items='center', margin='3px 0'))

        # ★ container 结构：控制区 + 表头 + _rows_box
        # _rows_box 是唯一包含预览行 checkbox 的容器
        self.container.children = (
            note, ctrl, range_row, header, self._rows_box)

    # ── 拖拽回调 ────────────────────────────────────────────────
    def drag_commit(self, lo, hi, target_val):
        lo = max(0, int(lo))
        hi = min(len(self._boxes)-1, int(hi))
        for i in range(lo, hi+1):
            if i < len(self._boxes):
                self._boxes[i].value = bool(target_val)

    # ── 获取选中（从 _selected_set 读）────────────────────────
    def get_selected(self):
        if not self._items:
            return None
        with self._cb_lock:
            indices = sorted(self._selected_set)
        return [self._items[i] for i in indices if i < len(self._items)]

    # ── 行状态更新入队 ─────────────────────────────────────────
    def mark(self, vid_id, status, reason=''):
        if not vid_id:
            return
        with self._pending_lock:
            self._pending_marks[vid_id] = (status, reason)

    # ── 补全数据入队（线程安全，不直接改 widget）──────────────
    def enqueue_enrichment(self, index, updated_item):
        with self._pending_lock:
            self._pending_enrichments[index] = updated_item

    # ── 主线程统一应用所有 pending（flush_queue 调用）─────────
    def apply_pending_marks(self):
        with self._pending_lock:
            marks       = dict(self._pending_marks)
            enrichments = dict(self._pending_enrichments)
            self._pending_marks.clear()
            self._pending_enrichments.clear()

        # 应用状态标记
        for vid_id, (st, reason) in marks.items():
            for i, item in enumerate(self._items):
                if item.get('id', '') == vid_id:
                    if i < len(self._content_widgets):
                        self._st_states[i] = (st, reason)
                        self._content_widgets[i].value = \
                            self._content_html(i, item, st, reason)
                    break

        # ★ 应用补全数据（主线程执行，无线程安全问题）
        for i, updated in enrichments.items():
            if i < len(self._items) and i < len(self._content_widgets):
                self._items[i] = updated
                st, reason = (self._st_states[i]
                              if i < len(self._st_states) else (None, ''))
                self._content_widgets[i].value = \
                    self._content_html(i, updated, st, reason)

    def clear(self):
        for old_cb in self._boxes:
            try:
                old_cb.unobserve_all()
            except Exception:
                pass
        self._items               = []
        self._boxes               = []
        self._content_widgets     = []
        self._st_states           = []
        self._saved_ids           = set()
        self._is_downloading      = False
        with self._cb_lock:
            self._selected_set.clear()
        with self._pending_lock:
            self._pending_marks.clear()
            self._pending_enrichments.clear()
        self._rows_box.children  = ()
        self.container.children  = ()