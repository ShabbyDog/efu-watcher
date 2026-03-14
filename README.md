# efu-watcher

A Python daemon that maintains a real-time [Everything (voidtools)](https://www.voidtools.com/) compatible EFU file list for a Linux filesystem, enabling fast and always-current file search from Windows via Everything's **File Lists** feature.

## Background

Voidtools Everything on Windows cannot natively index Linux/XFS filesystems efficiently. The EFU (Everything File List) format lets Everything consume a pre-built index, but existing tools only do periodic full scans. This daemon bridges the gap: it uses Linux `inotify` to watch the filesystem in real time and keeps the EFU file updated within seconds of any change.

## Architecture

```
inotify kernel events
        │
        ▼
InotifyWatcher (thread)
  │  normalized event dicts
  ▼
event_queue (Queue)
        │
        ▼
EventProcessor (thread)
  │  debounces + collapses events
  │  ├─ db.upsert / db.delete / db.rename_directory
  │  └─ efu_writer.patch_many / .append_line
  │
  ├─ rebuild_queue ──► DailyRebuilder (thread)
  │                       full_rebuild() once daily (or on-demand)
  │
  └─ SQLite DB ◄──────── Reconciler (startup, synchronous)
                              full scan → diff → rebuild EFU
```

### Threading model

| Thread | Role |
|---|---|
| `InotifyWatcher` | Blocking inotify read loop; pushes events to `event_queue` |
| `EventProcessor` | Drains queue, debounces, updates DB and EFU file |
| `DailyRebuilder` | Full EFU rebuild at a configured hour, or on-demand |
| Main | Startup, reconciliation, signal handling, joins threads on shutdown |

## EFU Format

CSV with header `Filename,Size,Date Modified,Date Created,Attributes`:

```
Filename,Size,Date Modified,Date Created,Attributes
"\\cube.local\Storage\movies\foo.mkv",4294967296,133005740975895728,132725030821962416,32
"\\cube.local\Storage\docs",0,133005740975895728,132725030821962416,16
```

- Paths use Windows UNC format with backslashes
- Timestamps are [Windows FILETIME](https://docs.microsoft.com/en-us/windows/win32/sysinfo/file-times) (100-nanosecond intervals since 1601-01-01)
- Full rebuilds write to a `.tmp` file then atomically `rename()` into place
- Single-line changes patch the file in-place using the cached `efu_line` for fast string search

## SQLite Schema

```sql
CREATE TABLE files (
    path          TEXT PRIMARY KEY,   -- absolute POSIX path
    size          INTEGER NOT NULL,
    date_modified INTEGER NOT NULL,   -- Windows FILETIME
    date_created  INTEGER NOT NULL,   -- Windows FILETIME
    attributes    INTEGER NOT NULL,   -- Windows FILE_ATTRIBUTE_* flags
    efu_line      TEXT NOT NULL       -- cached CSV line for fast patching
);
```

The `efu_line` column stores the exact formatted CSV line. On a change event the old value is used to locate and replace the line in the EFU file via string search, avoiding fragile line-number tracking.

## Installation

### Prerequisites

- Linux (inotify is Linux-only)
- Python 3.10+
- The filesystem to index mounted and accessible

### Steps

```bash
# 1. Clone and install
git clone https://github.com/yourname/efu-watcher.git /opt/efu-watcher
cd /opt/efu-watcher
python3 -m venv venv
venv/bin/pip install -e .

# 2. Create DB directory
sudo mkdir -p /var/lib/efu-watcher
sudo chown cube:cube /var/lib/efu-watcher

# 3. (Optional) create a config override file
sudo mkdir -p /etc/efu-watcher
sudo tee /etc/efu-watcher/config.env <<'EOF'
WATCH_ROOT=/mnt/cube/Storage
EFU_PATH=/mnt/cube/Storage/.everything_index.efu
DB_PATH=/var/lib/efu-watcher/index.db
UNC_HOST=cube.local
UNC_SHARE=Storage
EOF

# 4. Install and enable the systemd service
sudo cp efu-watcher.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now efu-watcher
```

### Configure Everything on Windows

1. Open Everything → **Tools → Options → Indexes → File Lists**
2. Add `\\cube.local\Storage\.everything_index.efu`
3. Everything will now index and search the file in real time

## Configuration

All settings are environment variables. Set them in the unit file or in `/etc/efu-watcher/config.env`:

| Variable | Default | Description |
|---|---|---|
| `WATCH_ROOT` | `/mnt/cube/Storage` | Filesystem path to watch |
| `EFU_PATH` | `$WATCH_ROOT/.everything_index.efu` | Output EFU file path |
| `DB_PATH` | `/var/lib/efu-watcher/index.db` | SQLite database path |
| `UNC_HOST` | `cube.local` | Windows UNC hostname |
| `UNC_SHARE` | `Storage` | Windows UNC share name |
| `DEBOUNCE_WINDOW` | `3.0` | Seconds to batch events before flushing |
| `DAILY_REBUILD_HOUR` | `3` | Hour (0–23) for daily full rebuild |
| `MAX_PATCH_SIZE_MB` | `200` | EFU files larger than this trigger rebuild instead of patch |
| `LOG_LEVEL` | `INFO` | Python logging level |

## Edge Cases Handled

| Scenario | Handling |
|---|---|
| **Recursive directory move** | `rename_directory()` updates all child paths in DB atomically; `patch_many()` replaces all EFU lines in one read-write cycle |
| **Rapid event bursts** | `EventProcessor` debounces with configurable window; collapses redundant events (e.g. CREATE+DELETE→no-op, multiple MODIFY→latest) |
| **Daemon restart / offline changes** | `Reconciler` does a full disk scan on startup, diffs against DB, updates stale entries, then rebuilds EFU |
| **Race window on new directory** | `InotifyWatcher` adds watches immediately on `CREATE+ISDIR`; `EventProcessor` scans directory contents on a create event to catch files in the race window |
| **MOVED_FROM without MOVED_TO** | Treated as DELETE after the debounce window expires |
| **MOVED_TO without MOVED_FROM** | Treated as CREATE |
| **Mount disappears** | `BindsTo=mnt-cube-Storage.mount` in the unit file causes systemd to stop and restart the service in sync with the mount |
| **Large EFU files** | Above `MAX_PATCH_SIZE_MB`, patch falls back to a full rebuild rather than loading the entire file into memory |
| **EFU file self-exclusion** | `.everything_index.efu` is explicitly excluded from the watch and index |

## Logs

```bash
# Follow live logs
journalctl -u efu-watcher -f

# Show logs since last boot
journalctl -u efu-watcher -b
```

## Development

```bash
# Run directly (requires Linux + inotify-simple)
WATCH_ROOT=/your/path EFU_PATH=/your/path/.index.efu DB_PATH=/tmp/test.db \
    UNC_HOST=myserver UNC_SHARE=share \
    python -m efu_watcher.main
```

The first three modules (`database.py`, `efu_writer.py`, `reconciler.py`) have no inotify dependency and can be unit-tested on any OS. Only `watcher.py` and `main.py` require Linux.

## License

MIT
