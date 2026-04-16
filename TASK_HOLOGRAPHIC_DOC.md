# YouTube 下载器任务全息文档（UI 视觉升级 + 勾选拖拽修复）

## 1. 任务目标
- 保留原文件不动。
- 在根目录生成新文件并改造：`/home/runner/work/YouTube/YouTube/yt_downloader_ui_v332.py`。
- 功能逻辑不变，仅升级 UI 样式与交互体验。
- 修复 Colab iframe 环境下拖拽批量勾选偶发卡死。

## 2. 文件产物
- 原文件（未改）：`/home/runner/work/YouTube/YouTube/yt_downloader.py`
- 新文件（本轮改）：`/home/runner/work/YouTube/YouTube/yt_downloader_ui_v332.py`

## 3. 视觉体系升级（Linear/Vercel 风格）
- 全局深色主题：
  - 背景：`#0d0d0f`
  - 卡片：`#18181b`
  - 边框：`1px #2a2a2e`
  - Accent：`#3b82f6`
- 统一按钮系统：
  - 主按钮：实心 accent
  - 次按钮：透明 + accent 边框
  - 危险按钮：透明 + 红色边框
  - 统一圆角、hover 提亮+轻微上移
- 模式区：改为 segmented/pill 风格（统一容器 + 选中高亮）
- 输入控件：统一深色输入样式与 focus ring
- 顶部说明区：改为 12px 低对比提示

## 4. 结果列表升级
- 行 hover 高亮：`#ffffff08`
- 状态列改 badge 风格：
  - 下载中：蓝色点
  - 完成：绿色 ✓
  - 失败：红色 ✗
- 标题列增加缩略图（40px 宽，16:9，圆角）
  - 优先使用 `id` 推导：`https://i.ytimg.com/vi/<id>/mqdefault.jpg`
- 复选框样式自定义（隐藏原生观感）

## 5. 进度体验升级
- 新增两条真实进度条（含百分比文字）：
  - 当前视频进度
  - 总进度
- 由状态回调驱动更新，不改变下载逻辑。

## 6. Colab 拖拽修复方案（根因级）
- 将拖拽核心事件落在列表容器 `.yt-rows-box`：
  - `pointerdown` / `pointermove` / `pointerup` / `pointercancel`
- 在 `pointerdown` 时：
  - `event.preventDefault()`
  - `setPointerCapture(pointerId)`
- 在 `pointerup/pointercancel` 时：
  - `releasePointerCapture(pointerId)`
  - 重置拖拽状态
- 修复点：即使鼠标移出 iframe，事件仍由捕获元素持续接收，避免“拖拽状态卡住”。

## 7. 功能不变性说明
- 搜索、预览、勾选、下载、暂停/继续/终止、日志、索引维护等核心流程未改语义。
- 本轮主要改动集中在：
  - 样式注入（`_THEME_JS`）
  - 列表与勾选拖拽前端脚本（`_DRAG_JS`）
  - 展示层渲染函数（`_row_html` / `_build`）
  - 状态栏视觉与进度回调桥接

## 8. 验证结果
- 语法校验：
  - `python -m py_compile /home/runner/work/YouTube/YouTube/yt_downloader_ui_v332.py` ✅

## 9. 后续可选增强
- 可继续补充：行级骨架屏、虚拟滚动、键盘快捷键（全选/反选）、更细粒度错误 badge。
