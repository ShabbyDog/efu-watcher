"""
efu-watcher: Real-time EFU file list daemon for voidtools Everything.

Monitors a Linux filesystem via inotify and maintains an always-current
EFU (Everything File List) for consumption by Everything on Windows.
"""

__version__ = "1.0.0"

# Default configuration — all overridable via environment variables in main.py
WATCH_ROOT = "/mnt/cube/Storage"
EFU_PATH = "/mnt/cube/Storage/.everything_index.efu"
DB_PATH = "/var/lib/efu-watcher/index.db"
UNC_HOST = "cube.local"
UNC_SHARE = "Storage"
DEBOUNCE_WINDOW = 3.0       # seconds to wait after last event before flushing
DAILY_REBUILD_HOUR = 3      # hour (0-23) at which to run the daily full rebuild
MAX_PATCH_SIZE_MB = 200     # above this, force full rebuild instead of patching
