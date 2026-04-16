        class _Logger:
            def debug(self_, msg):
                while pause_ev.is_set() and not stop_ev.is_set():
                    time.sleep(0.3)
                if stop_ev.is_set(): raise _StopDownload()
                # 进度百分比 → 驱动 StatusBar 小鱼，不写日志行
                if '[download]' in msg:
                    m = re.search(r'(\d+(?:\.\d+)?)%', msg)
                    if m:
                        pct  = float(m.group(1))
                        step = int(pct // 10) * 10
                        if step != last_step[0]:
                            last_step[0] = step
                            if status:
                                status.update_progress(pct, idx, n, title)
                # 反爬检测，不写日志
                if _SB_RE.search(msg):
                    sb_log.append(msg[:80])

            def warning(self_, msg):
                if any(k in msg for k in _WARN_SKIP): return
                if '429' in msg and 'subtitle' in msg.lower():
                    log.write('字幕限速，已跳过'); return
                log.write(f'注意: {msg.strip()[:88]}')

            def error(self_, msg):
                if '429' in msg and 'subtitle' in msg.lower():
                    log.write('字幕失败，已跳过'); return
                log.write(f'错误: {msg.strip()[:88]}')