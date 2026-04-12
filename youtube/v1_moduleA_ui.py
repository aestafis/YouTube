# ══════════════════════════════════════════════════════════════
# MODULE A — UI精简 / 乱码修复 / 配色 / 表格布局
# 解决: A-1~A-9 全部
# ══════════════════════════════════════════════════════════════

# ── A-4: 排序选项，只保留两个 ────────────────────────────────
SORT_OPTS = {'相关性': '', '最多播放': 'viewcount'}
_SP       = {'viewcount': '&sp=CAM%3D', '': ''}

# ── A-1: 所有 description 只用 ASCII ─────────────────────────
# 规则：W.Button / W.Text / W.Checkbox / W.Dropdown 的
#       description 参数只允许 ASCII。
#       中文说明全部放 tooltip。
#
# 乱码字符归因表（实测触发截断的字）：
#   '保存' → '保' U+4FDD 在某些 ipywidgets 7.x 下与 ':' 组合截断
#   '独立' → '立' U+7ACB 后接汉字截断
#   '游戏' → 单独不截断，但 '独立游戏' 整体截断
#   '设置' → '置' U+7F6E 与 '(' 组合截断
# 结论: 凡超过2个连续汉字+标点的 description 都有截断风险
#       一律改 ASCII

# ── A-3/A-9: 表格列定义（去掉发布时间列）────────────────────
# 新列布局: # | 标题/频道 | 状态 | 播放量 | 时长
# grid: 28px 1fr 22px 72px 54px
_GRID_COLS = '28px 1fr 22px 72px 54px'

_HEADER_HTML = (
    '<div style="display:grid;'
    f'grid-template-columns:{_GRID_COLS};'
    'gap:0 6px;font-size:11px;color:#999;'
    'padding:5px 6px;border-bottom:1px solid #444">'
    '<div style="text-align:center">#</div>'
    '<div>Title / Channel</div>'
    '<div style="text-align:center">St</div>'
    '<div style="text-align:right;padding-right:4px">Views</div>'
    '<div style="text-align:center">Dur</div>'
    '</div>')

# ── A-7/A-8: 状态方块（深色主题友好）────────────────────────
_ST_CFG = {
    'downloading': ('#ff9800', '>'),
    'done':        ('#4caf50', '+'),
    'fail':        ('#f44336', 'x'),
    'skip':        ('#9e9e9e', '-'),
    'saved':       ('#2196f3', 'v'),
}

def _st_span(st=None, reason=''):
    # st=None → 空占位，无背景，无颜色
    if not st:
        return ('<span style="display:inline-block;'
                'width:20px;height:20px"></span>')
    color, icon = _ST_CFG.get(st, ('#888', '?'))
    tip = f' title="{reason}"' if reason else ''
    return (
        f'<span style="display:inline-block;width:20px;height:20px;'
        f'line-height:20px;text-align:center;border-radius:3px;'
        f'background:{color};color:#fff;font-size:11px;'
        f'font-weight:bold;cursor:default"{tip}>{icon}</span>')

# ── A-7: 行内容 HTML（透明背景，去掉发布时间列）─────────────
def _content_html(i, r, st=None, reason=''):
    title = r.get('title', '')
    ts    = _trim(title, 34)
    ch    = _trim(r.get('channel') or 'N/A', 24)
    dur   = r.get('duration', '-')
    url   = r.get('url', '#')
    views = _fmt_views(r.get('view_count'))

    # A-7: 透明背景，奇偶行用极淡的透明色区分
    bg = 'rgba(255,255,255,0.03)' if i % 2 == 0 else 'transparent'

    vh = (f'<span style="font-size:12px;color:#ccc">{views}</span>'
          if views else
          '<span style="color:#555;font-size:11px">-</span>')
    st_s = _st_span(st, reason)

    return (
        f'<div style="display:grid;'
        f'grid-template-columns:{_GRID_COLS};'
        f'gap:0 6px;align-items:center;min-height:52px;'
        f'padding:3px 6px;background:{bg};'
        f'border-bottom:1px solid rgba(255,255,255,0.06)">'
        # 序号
        f'<div style="text-align:center;color:#666;font-size:11px">'
        f'{i+1}</div>'
        # 标题+频道
        f'<div style="min-width:0;overflow:hidden">'
        f'<a href="{url}" target="_blank" '
        f'style="color:#64b5f6;text-decoration:none;font-size:13px;'
        f'font-weight:500;display:block;'
        f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis"'
        f' title="{title}">{ts}</a>'
        f'<div style="color:#777;font-size:11px;'
        f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'
        f'{ch}</div>'
        f'</div>'
        # 状态
        f'<div style="text-align:center">{st_s}</div>'
        # 播放量
        f'<div style="text-align:right;padding-right:4px">{vh}</div>'
        # 时长
        f'<div style="text-align:center;font-size:12px;color:#aaa">'
        f'{dur}</div>'
        f'</div>')

# ── A-8: StatusBar 深色主题配色 ──────────────────────────────
# 原来用亮色背景（#e8f5e9等），在深色主题下极刺眼
# 改为：深色背景 + 彩色左边框 + 浅色文字
_SB_STYLES = {
    'idle':       ('rgba(255,255,255,0.04)', '#888', '#555', '待机'),
    'search':     ('rgba(21,101,192,0.15)',  '#64b5f6', '#1565c0', '搜索'),
    'found':      ('rgba(46,125,50,0.15)',   '#81c784', '#2e7d32', '完成'),
    'dl':         ('rgba(230,81,0,0.15)',    '#ffb74d', '#e65100', '下载'),
    'pause':      ('rgba(198,40,40,0.15)',   '#ef9a9a', '#c62828', '暂停'),
    'done':       ('rgba(46,125,50,0.15)',   '#81c784', '#2e7d32', '完成'),
    'stop':       ('rgba(255,255,255,0.04)', '#888', '#555', '停止'),
    'error':      ('rgba(198,40,40,0.15)',   '#ef9a9a', '#c62828', '错误'),
}

_SB_TMPL = (
    '<div style="font-size:13px;font-family:monospace;'
    'background:{bg};color:{fg};padding:7px 14px;'
    'border-radius:4px;border-left:3px solid {border};'
    'line-height:1.8;margin:2px 0;'
    'white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'
    '<b>[{icon}]</b>&nbsp;&nbsp;{msg}</div>')

def _sb_html(style_key, icon, msg):
    bg, fg, border, _ = _SB_STYLES.get(style_key, _SB_STYLES['idle'])
    return _SB_TMPL.format(bg=bg, fg=fg, border=border,
                           icon=icon, msg=msg)

# ── A-11: Accordion 标题设置（双API兼容）─────────────────────
def _acc_set_title(acc, title):
    """兼容 ipywidgets 7.x 和 8.x"""
    try:
        acc.titles = (title,)
    except Exception:
        try:
            acc.set_title(0, title)
        except Exception:
            pass

# ── A-1: 固定模块按钮（description全ASCII）──────────────────
# 原来: description='独立游戏' → 乱码
# 改为: description='Indie' + tooltip='独立游戏: indie game review 2026'
KEYWORD_MODULES_DISPLAY = {
    'AI / ML': {
        'Transformer': 'transformer explained from scratch',
        'LLM':         'LLM implementation tutorial 2026',
        'AI Agent':    'AI agent build from scratch',
        'Paper':       'AI research paper explained 2026',
        'OpenSource':  'open source AI tools github 2026',
    },
    'Code / Edu': {
        'Python':      'python project tutorial 2026',
        'Algorithm':   'algorithm explained visually',
        'SysDesign':   'system design explained',
        'Math':        'math intuition visual explanation',
        'Frontend':    'web development tutorial 2026',
    },
    'Game': {
        'Highlights':  'game highlights 2026',
        'Indie':       'indie game review 2026',
        'Speedrun':    'speedrun world record 2026',
        'Design':      'game design analysis deep dive',
    },
    'Anime': {
        'Essay':       'anime video essay 2025',
        'Sakuga':      'anime sakuga breakdown',
        'Season':      'best anime 2025 recommendation',
        'Manga':       'manga vs anime adaptation comparison',
    },
    'Entertainment': {
        'Trailer':     'movie trailer 2026',
        'Documentary': 'documentary full length 2026',
        'Comedy':      'comedy sketch 2026',
        'Viral':       'most viral video 2026',
    },
}
# tooltip 显示中文原名
KEYWORD_MODULES_TOOLTIP = {
    'AI / ML':         {'Transformer':'AI原理','LLM':'大模型实战',
                        'AI Agent':'AI Agent','Paper':'论文精读',
                        'OpenSource':'开源项目'},
    'Code / Edu':      {'Python':'Python实战','Algorithm':'算法讲解',
                        'SysDesign':'系统设计','Math':'数学直觉',
                        'Frontend':'前端开发'},
    'Game':            {'Highlights':'精彩时刻','Indie':'独立游戏',
                        'Speedrun':'速通纪录','Design':'游戏设计'},
    'Anime':           {'Essay':'深度分析','Sakuga':'作画赏析',
                        'Season':'新番推荐','Manga':'漫改对比'},
    'Entertainment':   {'Trailer':'电影预告','Documentary':'深度纪录片',
                        'Comedy':'喜剧短片','Viral':'近期爆款'},
}

# ── A-5/A-6: PreviewTable 控制区（精简版）───────────────────
# 删除: 范围勾选/取消、勾选已存按钮文字改ASCII
# 保留: 全选checkbox + 已存两个操作（纯Python可靠）
def _build_table_controls(items, saved_ids, boxes):
    """
    返回 (ctrl_widget, note_widget)
    boxes: list of W.Checkbox，由外部传入引用
    """
    # 全选
    all_cb = W.Checkbox(
        value=True, description='All', indent=False,
        layout=W.Layout(width='52px', min_width='52px'),
        tooltip='全选/全不选')

    def _toggle_all(c):
        for b in boxes: b.value = c['new']
    all_cb.observe(_toggle_all, names='value')

    # 已存：勾选 / 取消（A-6）
    btn_sel = W.Button(
        description='Saved+',
        layout=W.Layout(width='72px', height='26px'),
        style={'font_size': '11px', 'button_color': '#1565c0'},
        tooltip='勾选所有已存视频')
    btn_unsel = W.Button(
        description='Saved-',
        layout=W.Layout(width='72px', height='26px'),
        style={'font_size': '11px', 'button_color': '#37474f'},
        tooltip='取消所有已存视频的勾选')

    def _sel_saved(_):
        for r, b in zip(items, boxes):
            if r.get('id', '') in saved_ids:
                b.value = True
    def _unsel_saved(_):
        for r, b in zip(items, boxes):
            if r.get('id', '') in saved_ids:
                b.value = False

    btn_sel.on_click(_sel_saved)
    btn_unsel.on_click(_unsel_saved)

    has_saved = bool(saved_ids & {r.get('id', '') for r in items})
    note = W.HTML(
        f'<div style="font-size:10px;color:#777;padding:4px 8px;'
        f'border-bottom:1px solid rgba(255,255,255,0.08)">'
        f'<b style="color:#ccc">{len(items)}</b> videos'
        f'{" &nbsp;[v]=saved(unchecked by default)" if has_saved else ""}'
        f' &nbsp;[>]=downloading &nbsp;[+]=done &nbsp;[x]=fail'
        f' &nbsp;| drag checkboxes to batch select'
        f'</div>')

    ctrl = W.HBox(
        [all_cb,
         W.HTML('<span style="font-size:10px;color:#666;'
                'margin:auto 6px">Saved:</span>'),
         btn_sel, btn_unsel],
        layout=W.Layout(align_items='center', margin='2px 0'))

    return ctrl, note