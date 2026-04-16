# ══════════════════════════════════════════════════════════════
# MODULE 2 — UI 文字渲染
# 解决: P0-3(Accordion星号) / P2-1(刷新按钮命名)
# ══════════════════════════════════════════════════════════════

def _make_accordion(children_widget, short_title, tip_html=''):
    """
    ★ P0-3 修复：
    - 标题只用短字符串（不超过8个汉字，约24字节），避免截断
    - 如需说明文字，传入 tip_html，插在内容区顶部
    - 同时用 .titles = (...) 新写法，兼容 ipywidgets 7.x 和 8.x
    """
    if tip_html:
        inner = W.VBox([
            W.HTML(tip_html),
            children_widget
        ], layout=W.Layout(padding='4px'))
    else:
        inner = W.VBox([children_widget],
                       layout=W.Layout(padding='4px'))

    acc = W.Accordion(children=[inner],
                      layout=W.Layout(width='100%', margin='2px 0'))
    # ★ 兼容写法：先尝试新 API，失败则用旧 API
    try:
        acc.titles = (short_title,)
    except Exception:
        try:
            acc.set_title(0, short_title)
        except Exception:
            pass
    acc.selected_index = None
    return acc


def _build_keyword_modules(w_query, KEYWORD_MODULES):
    """
    ★ P0-3 修复：固定模块 Accordion 标题改短
    说明文字移到内容区顶部 HTML
    """
    mod_rows = []
    for cat, kws in KEYWORD_MODULES.items():
        btns = []
        for lbl, kw in kws.items():
            b = W.Button(
                description=lbl,
                layout=W.Layout(width='auto', height='26px'),
                style={'font_size': '11px'},
                tooltip=f'搜索词: {kw}')
            def _ok(_b, _kw=kw):
                yr = datetime.now().year
                ky = _kw.replace('2026', str(yr)).replace('2025', str(yr-1))
                w_query.value = ky
            b.on_click(_ok)
            btns.append(b)

        mod_rows.append(W.VBox([
            W.HTML(
                f'<div style="font-size:11px;font-weight:600;'
                f'color:#555;padding:2px 0">{cat}</div>'),
            W.HBox(btns, layout=W.Layout(
                flex_flow='row wrap', margin='0 0 4px'))
        ]))

    tip_html = (
        '<div style="font-size:10px;color:#888;'
        'padding:2px 0 4px;border-bottom:1px solid #eee;margin-bottom:4px">'
        '点击按钮填入搜索词 · 悬停查看原始搜索词 · 年份自动替换为当前年</div>')

    mod_container = W.VBox(mod_rows)
    # ★ 短标题，避免截断
    acc = _make_accordion(mod_container, '固定模块', tip_html)
    return acc


def _build_settings_accordion(Cfg, W):
    """
    ★ P0-3 修复：设置 Accordion 标题改短
    说明文字移到内容区顶部
    """
    S = {'description_width': '65px'}
    L = W.Layout

    w_cookie = W.Text(
        value=Cfg.COOKIE,
        description='Cookie:',
        style={'description_width': '55px'},
        layout=L(width='97%'),
        tooltip='YouTube Cookie 文件路径，需手动上传到 Google Drive')

    w_save = W.Text(
        value=Cfg.SAVE_DIR,
        description='保存目录:',
        style={'description_width': '55px'},
        layout=L(width='97%'),
        tooltip='视频保存根目录，不存在会自动创建')

    w_maxmb = W.IntSlider(
        value=0, min=0, max=10000, step=100,
        description='大小上限:',
        style=S,
        layout=L(width='55%'),
        continuous_update=False,
        tooltip='单个视频文件大小上限(MB)，0 = 不限制')

    # ★ 实时显示当前值含义
    w_maxmb_label = W.HTML(
        value='<span style="font-size:11px;color:#888;margin-left:6px">'
              '不限制</span>')

    def _update_label(c):
        v = c['new']
        if v == 0:
            w_maxmb_label.value = (
                '<span style="font-size:11px;color:#2e7d32;margin-left:6px">'
                '不限制</span>')
        else:
            w_maxmb_label.value = (
                f'<span style="font-size:11px;color:#e65100;margin-left:6px">'
                f'最大 {v} MB</span>')
    w_maxmb.observe(_update_label, names='value')

    w_subtitle = W.Checkbox(
        value=False, description='下载字幕', indent=False,
        layout=L(width='auto'),
        tooltip='同时下载 zh-Hans / zh-Hant / en 字幕文件')

    w_skip_saved = W.Checkbox(
        value=True, description='跳过已存', indent=False,
        layout=L(width='auto'),
        style={'description_width': 'auto'},
        tooltip='关键词搜索时自动过滤已下载过的视频（推荐开启）')

    w_enrich = W.Checkbox(
        value=True, description='补全发布时间', indent=False,
        layout=L(width='auto'),
        style={'description_width': 'auto'},
        tooltip='搜索完成后异步补全发布时间和频道名（会增加约5-15秒）')

    w_reset_btn = W.Button(
        description='重置记录',
        button_style='warning',
        layout=L(width='90px'),
        tooltip='清空历史下载记录（不删除实际文件）')

    w_reset_idx = W.Checkbox(
        value=False, description='同时清空已存索引', indent=False,
        layout=L(width='auto'),
        style={'description_width': 'auto'},
        tooltip='同时删除 .yt_index.json，预览列表中的蓝色[v]标记会消失')

    tip_html = (
        '<div style="font-size:10px;color:#888;'
        'padding:2px 0 4px;border-bottom:1px solid #eee;margin-bottom:6px">'
        'Cookie: 需从浏览器导出并上传到 Drive · '
        '大小上限 0=不限制 · 跳过已存避免重复下载</div>')

    content = W.VBox([
        w_cookie, w_save,
        W.HBox([w_maxmb, w_maxmb_label],
               layout=L(align_items='center')),
        W.HBox([w_subtitle,
                W.HTML('&nbsp;&nbsp;'),
                w_skip_saved,
                W.HTML('&nbsp;&nbsp;'),
                w_enrich],
               layout=L(align_items='center')),
        W.HBox([w_reset_btn,
                W.HTML('&nbsp;'),
                w_reset_idx],
               layout=L(align_items='center')),
    ])

    # ★ 短标题
    acc = _make_accordion(content, '设置', tip_html)

    widgets = {
        'cookie':      w_cookie,
        'save':        w_save,
        'maxmb':       w_maxmb,
        'subtitle':    w_subtitle,
        'skip_saved':  w_skip_saved,
        'enrich':      w_enrich,
        'reset_btn':   w_reset_btn,
        'reset_idx':   w_reset_idx,
    }
    return acc, widgets


def _make_button_row(w):
    """
    ★ P2-1: 刷新按钮改为'重启刷新'，tooltip说明
    所有按钮 description 只用安全字符
    """
    L = W.Layout

    w_prev = W.Button(
        description='搜索预览',
        button_style='info',
        layout=L(width='88px'),
        tooltip='搜索并显示预览列表')

    w_cancel_prev = W.Button(
        description='取消',
        button_style='warning',
        layout=L(width='60px'),
        disabled=True,
        tooltip='取消当前搜索')

    w_dl = W.Button(
        description='开始下载',
        button_style='success',
        layout=L(width='88px'),
        tooltip='下载勾选的视频')

    w_pause = W.Button(
        description='暂停',
        layout=L(width='72px'),
        disabled=True,
        style={'button_color': '#f57f17'},
        tooltip='当前视频下载完成后暂停')

    w_resume = W.Button(
        description='继续',
        button_style='success',
        layout=L(width='72px'),
        disabled=True,
        tooltip='继续下载下一个视频')

    w_stop = W.Button(
        description='终止',
        button_style='danger',
        layout=L(width='72px'),
        disabled=True,
        tooltip='立即终止当前下载')

    # ★ P2-1: 改为'重启刷新'，语义明确
    w_refresh = W.Button(
        description='重启刷新',
        layout=L(width='88px'),
        style={'button_color': '#607d8b'},
        tooltip='状态长时间不更新时点击：重新注入 2s 自动刷新定时器')

    w_clear_preview = W.Button(
        description='清空预览',
        layout=L(width='80px'),
        style={'button_color': '#607d8b'},
        tooltip='清空预览列表')

    w_clear_log = W.Button(
        description='清空日志',
        layout=L(width='80px'),
        style={'button_color': '#607d8b'},
        tooltip='清空下载日志')

    btn_row = W.HBox(
        [w_prev, w_cancel_prev, w_dl,
         W.HTML('<span style="width:8px;display:inline-block"></span>'),
         w_pause, w_resume, w_stop,
         W.HTML('<span style="width:8px;display:inline-block"></span>'),
         w_refresh,
         W.HTML('<span style="width:8px;display:inline-block"></span>'),
         w_clear_preview, w_clear_log],
        layout=W.Layout(
            margin='6px 0',
            flex_flow='row wrap',
            align_items='center'))

    btns = {
        'prev':         w_prev,
        'cancel_prev':  w_cancel_prev,
        'dl':           w_dl,
        'pause':        w_pause,
        'resume':       w_resume,
        'stop':         w_stop,
        'refresh':      w_refresh,
        'clear_preview':w_clear_preview,
        'clear_log':    w_clear_log,
    }
    return btn_row, btns