"""
efu-watcher: Real-time EFU file list daemon for voidtools Everything.

Monitors a Linux filesystem via inotify and maintains an always-current
EFU (Everything File List) for consumption by Everything on Windows.
"""

__version__ = "1.0.0"

# Default configuration — all overridable via environment variables in main.py
# These are fallback values only; set real values in /etc/efu-watcher/config.env
WATCH_ROOT = "/mnt/share"
EFU_PATH = "/mnt/share/.everything_index.efu"
DB_PATH = "/var/lib/efu-watcher/index.db"
UNC_HOST = "fileserver.local"
UNC_SHARE = "share"
# Optional override: set WIN_PATH_PREFIX to use a drive letter (e.g. "Z:") or
# any custom prefix instead of constructing \\UNC_HOST\UNC_SHARE automatically.
WIN_PATH_PREFIX = ""
DEBOUNCE_WINDOW = 3.0       # seconds to wait after last event before flushing
DAILY_REBUILD_HOUR = 3      # hour (0-23) at which to run the daily full rebuild
MAX_PATCH_SIZE_MB = 200     # above this, force full rebuild instead of patching
