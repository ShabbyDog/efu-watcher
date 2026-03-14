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
"\\fileserver.local\share\movies\foo.mkv",4294967296,133005740975895728,132725030821962416,32
"\\fileserver.local\share\docs",0,133005740975895728,132725030821962416,16
```

Or with a mapped drive letter (`WIN_PATH_PREFIX=Z:`):

```
Filename,Size,Date Modified,Date Created,Attributes
"Z:\movies\foo.mkv",4294967296,133005740975895728,132725030821962416,32
"Z:\docs",0,133005740975895728,132725030821962416,16
```

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
git clone https://github.com/ShabbyDog/efu-watcher.git /opt/efu-watcher
cd /opt/efu-watcher
python3 -m venv venv
venv/bin/pip install -e .

# 2. Create a dedicated service user and DB directory
sudo useradd -r -s /sbin/nologin efu-watcher
sudo mkdir -p /var/lib/efu-watcher
sudo chown efu-watcher:efu-watcher /var/lib/efu-watcher

# 3. Create your site config from the example
sudo mkdir -p /etc/efu-watcher
sudo cp config.env.example /etc/efu-watcher/config.env
sudo editor /etc/efu-watcher/config.env   # set WATCH_ROOT, UNC_HOST, etc.

# 4. Edit the service file to set WantsMountsFor and ReadWritePaths
#    to match your WATCH_ROOT, then install it
sudo editor efu-watcher.service
sudo cp efu-watcher.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now efu-watcher
```

### Configure Everything on Windows

1. Open Everything → **Tools → Options → Indexes → File Lists**
2. Add the network path to `.everything_index.efu`:
   - UNC style: `\\fileserver.local\share\.everything_index.efu`
   - Drive letter style (if the share is mapped as e.g. `Z:`): `Z:\.everything_index.efu`
3. Tick **Automatically update file lists**
4. Everything will index and search the share in real time

## Configuration

All settings live in `/etc/efu-watcher/config.env`. Copy `config.env.example` as a starting point — every option is documented there.

| Variable | Default | Description |
|---|---|---|
| `WATCH_ROOT` | `/mnt/share` | Filesystem path to watch |
| `EFU_PATH` | `$WATCH_ROOT/.everything_index.efu` | Output EFU file path |
| `DB_PATH` | `/var/lib/efu-watcher/index.db` | SQLite database path |
| `WIN_PATH_PREFIX` | _(unset)_ | If set, overrides `UNC_HOST`+`UNC_SHARE` entirely. Use `Z:` for a mapped drive or `\\server\share` for an explicit UNC path. |
| `UNC_HOST` | `fileserver.local` | Windows UNC hostname (ignored if `WIN_PATH_PREFIX` is set) |
| `UNC_SHARE` | `share` | Windows UNC share name (ignored if `WIN_PATH_PREFIX` is set) |
| `DEBOUNCE_WINDOW` | `3.0` | Seconds to batch events before flushing |
| `DAILY_REBUILD_HOUR` | `3` | Hour (0–23) for daily full rebuild |
| `MAX_PATCH_SIZE_MB` | `200` | EFU files larger than this trigger rebuild instead of patch |
| `LOG_LEVEL` | `INFO` | Python logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |

> **Note:** `WantsMountsFor=` in the service file and `ReadWritePaths=` are systemd directives that do not read environment variables. Edit the service file directly to match your `WATCH_ROOT`, then run `sudo systemctl daemon-reload`.

### Changing WIN_PATH_PREFIX

If you change `WIN_PATH_PREFIX` after the daemon has already built its index, simply restart the service. The daemon detects the prefix change, recomputes all cached path values in the database, and rebuilds the EFU automatically — no manual database deletion required.

## Service file notes

The `WantsMountsFor=` directive tells systemd to start the service after the mount unit that owns `WATCH_ROOT`, without needing to know the exact unit name. Change it to match your path. If the mount is a subdirectory within a larger mount (not a mountpoint itself), the daemon's built-in retry loop handles availability checks regardless.

## Edge Cases Handled

| Scenario | Handling |
|---|---|
| **Recursive directory move** | `rename_directory()` updates all child paths in DB atomically; `patch_many()` replaces all EFU lines in one read-write cycle |
| **Rapid event bursts** | `EventProcessor` debounces with configurable window; collapses redundant events (e.g. CREATE+DELETE→no-op, multiple MODIFY→latest) |
| **Daemon restart / offline changes** | `Reconciler` does a full disk scan on startup, diffs against DB, updates stale entries, then rebuilds EFU |
| **Race window on new directory** | `InotifyWatcher` adds watches immediately on `CREATE+ISDIR`; `EventProcessor` scans directory contents on a create event to catch files in the race window |
| **MOVED_FROM without MOVED_TO** | Treated as DELETE after the debounce window expires |
| **MOVED_TO without MOVED_FROM** | Treated as CREATE |
| **Mount not yet available** | `wait_for_mount()` retries every 30 seconds; `WantsMountsFor=` in the unit file orders startup after the relevant mount |
| **WIN_PATH_PREFIX change** | Detected via stored meta value in DB; all `efu_line` values recomputed automatically on next start |
| **Non-UTF-8 filenames** | Detected via surrogate escape check and skipped with a warning — these cannot be displayed on Windows |
| **inotify event queue overflow** | `IN_Q_OVERFLOW` triggers a full rebuild rather than silently losing events |
| **Large EFU files** | Above `MAX_PATCH_SIZE_MB`, patch falls back to a full rebuild |
| **EFU file self-exclusion** | `.everything_index.efu` is explicitly excluded from the watch and index |

## Logs

```bash
# Follow live logs
journalctl -u efu-watcher -f

# Show logs since last boot
journalctl -u efu-watcher -b
```

Normal startup produces output like:

```
INFO  efu-watcher starting
INFO  WATCH_ROOT=/mnt/share  EFU_PATH=...  DB_PATH=...
INFO  WIN_PREFIX=Z:  DEBOUNCE=3.0s  REBUILD_HOUR=3
INFO  InotifyWatcher starting, adding watches for /mnt/share
INFO  Reconciler starting full scan of /mnt/share
INFO  Disk scan complete: 450000 entries in 18.2s
INFO  Reconciliation: +450000 new, -0 deleted, ~0 modified
INFO  Full EFU rebuild complete: 450000 records
INFO  Reconciler finished in 24.1s (DB now has 450000 entries)
INFO  EventProcessor started (debounce=3.0s)
INFO  efu-watcher running
```

After that, each batch of filesystem changes logs one line:

```
INFO  Processing batch of 3 events
```

## Development

```bash
# Run directly (requires Linux + inotify-simple)
WATCH_ROOT=/your/path \
EFU_PATH=/your/path/.everything_index.efu \
DB_PATH=/tmp/test.db \
UNC_HOST=myserver \
UNC_SHARE=share \
    python -m efu_watcher.main
```

`database.py`, `efu_writer.py`, and `reconciler.py` have no inotify dependency and can be unit-tested on any OS. Only `watcher.py` and `main.py` require Linux.

## License

MIT
