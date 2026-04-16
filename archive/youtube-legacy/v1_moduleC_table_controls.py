# ══════════════════════════════════════════════════════════════
# MODULE C — PreviewTable 控制区（最终版）
# 修复: 已存按钮改为单toggle / description全ASCII /
#       删除补全相关代码 / 删除范围按钮
# ══════════════════════════════════════════════════════════════

# PreviewTable.__init__ 修订：删除补全相关
def __init__(self):
    self._items           = []
    self._boxes           = []
    self._content_widgets = []
    self._st_states       = []
    self._selected_set    = set()
    self._cb_lock         = threading.Lock()
    self._pending_marks   = {}      # 删除 _pending_enrichments
    self._pending_lock    = threading.Lock()
    self._saved_ids       = set()
    self._is_downloading  = False
    self._saved_toggled   = False   # toggle 状态：False=已存未选中
    self._rows_box  = W.VBox(layout=W.Layout(width='100%'))
    self.container  = W.VBox(layout=W.Layout(width='100%'))

# render() 里控制区部分（替换原来的控制区代码）：
def _render_controls(self, items):
    # 全选（description纯ASCII）
    all_cb = W.Checkbox(
        value=True, description='All', indent=False,
        layout=W.Layout(width='52px', min_width='52px'),
        tooltip='全选 / 全不选')

    def _toggle_all(c):
        for b in self._boxes: b.value = c['new']
    all_cb.observe(_toggle_all, names='value')

    # ★ 已存：单个 toggle 按钮
    self._saved_toggled = False   # render时重置状态
    btn_saved = W.Button(
        description='Saved+',
        layout=W.Layout(width='80px', height='26px'),
        style={'font_size':'11px', 'button_color':'#1565c0'},
        tooltip='Saved+: 勾选所有已存  /  Saved-: 取消所有已存的勾选')

    def _toggle_saved(_):
        self._saved_toggled = not self._saved_toggled
        val = self._saved_toggled
        for r, b in zip(self._items, self._boxes):
            if r.get('id','') in self._saved_ids:
                b.value = val
        btn_saved.description = 'Saved-' if val else 'Saved+'
        btn_saved.style.button_color = '#c62828' if val else '#1565c0'
    btn_saved.on_click(_toggle_saved)

    has_saved = bool(self._saved_ids & {r.get('id','') for r in items})
    note = W.HTML(
        f'<div style="font-size:10px;color:#777;padding:4px 8px;'
        f'border-bottom:1px solid rgba(255,255,255,0.08)">'
        f'<b style="color:#ccc">{len(items)}</b> results'
        f'{" &nbsp;[v]=saved(unchecked)" if has_saved else ""}'
        f'&nbsp; [>]=DL &nbsp;[+]=done &nbsp;[x]=fail'
        f'&nbsp; | drag to batch select'
        f'</div>')

    ctrl = W.HBox(
        [all_cb,
         W.HTML('<span style="font-size:10px;color:#666;'
                'margin:auto 6px">|</span>'),
         btn_saved],
        layout=W.Layout(align_items='center', margin='3px 0'))

    return note, ctrl

# apply_pending_marks() 修订：删除补全部分
def apply_pending_marks(self):
    with self._pending_lock:
        marks = dict(self._pending_marks)
        self._pending_marks.clear()
        # 删除 enrichments 相关
    for vid_id, (st, reason) in marks.items():
        for i, item in enumerate(self._items):
            if item.get('id','') == vid_id:
                if i < len(self._content_widgets):
                    self._st_states[i] = (st, reason)
                    self._content_widgets[i].value = \
                        _content_html(i, item, st, reason)
                break