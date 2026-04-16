        # 日志折叠面板（默认展开）
        _log_acc = W.Accordion(
            children=[self._log.widget()],
            layout=W.Layout(width='100%', margin='0'))
        _log_acc.set_title(0, '下载日志（点此折叠 / 展开）')
        _log_acc.selected_index = 0