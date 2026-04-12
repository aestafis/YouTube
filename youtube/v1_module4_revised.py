# ══════════════════════════════════════════════════════════════
# MODULE 4 REVISED — 补全启动时序修复
# 在 _on_preview 的 _search() 线程里，
# 补全启动通过 put_cb 延迟到 render() 之后执行
# ══════════════════════════════════════════════════════════════

# 在 _search() 线程里，找到这段：
#   self._last_results = res
#   self._auto_flush()
#   _apply_enrichment(...)   ← 错误：此时 render 可能还没执行
#
# 修改为：
#   self._last_results = res
#   if enrich_on and res:
#       # ★ put_cb 保证在 flush_queue 执行 render() 之后才调用
#       def _start_enrich_cb():
#           self._enricher.start(
#               items       = self._table._items,  # render后的items
#               cookie_path = cookie,
#               enqueue_fn  = self._table.enqueue_enrichment,
#               on_done     = lambda n: (
#                   self._log.write(
#                       f'补全完成: {n} 个视频发布时间/频道名已更新')
#                   if n > 0 else None),
#               enabled     = True)
#       self._uiq.put_cb(_start_enrich_cb)
#   self._auto_flush()
#
# 执行顺序：
#   _auto_flush() 调度 _flush_queue 到主线程
#   _flush_queue 先执行 render(self._last_results)
#   render 完成后 table._items 已填充
#   _flush_queue 再执行 callbacks 里的 _start_enrich_cb
#   此时 table._items 已就绪，补全安全启动

# 以下是完整的修订版 _search() 函数内补全部分（替换原来的）：

def _search_enrich_patch(self, res, cookie, enrich_on):
    """
    在 _search() 线程里，搜索完成后的补全启动逻辑。
    self._last_results 已设置，调用此函数后再 auto_flush。
    """
    if enrich_on and res:
        def _start_enrich_cb():
            # ★ 此时 render 已执行，table._items 已就绪
            if not self._table._items:
                return   # 安全检查
            self._enricher.start(
                items       = list(self._table._items),
                cookie_path = cookie,
                enqueue_fn  = self._table.enqueue_enrichment,
                on_done     = lambda n: (
                    self._log.write(
                        f'补全完成: {n} 个视频'
                        f'发布时间/频道名已更新') if n > 0 else None),
                enabled     = True)
        self._uiq.put_cb(_start_enrich_cb)