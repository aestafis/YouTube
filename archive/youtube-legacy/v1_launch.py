try: _INSTANCE._stop_ev.set(); _INSTANCE._pause_ev.clear()
except: pass
_INSTANCE = Dashboard(); _INSTANCE.launch()