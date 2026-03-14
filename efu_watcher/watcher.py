"""
inotify-based filesystem watcher for efu-watcher.

Watches the entire directory tree under watch_root recursively.
Translates raw inotify events into normalized dicts and pushes them
onto the shared event queue for processing by EventProcessor.
"""

import logging
import os
import queue
import threading
import time
from typing import Optional

try:
    import inotify_simple
    from inotify_simple import flags as iflags
    INOTIFY_AVAILABLE = True
except ImportError:
    INOTIFY_AVAILABLE = False
    inotify_simple = None  # type: ignore[assignment]

log = logging.getLogger(__name__)

_WATCH_FLAGS = 0
if INOTIFY_AVAILABLE:
    _WATCH_FLAGS = (
        iflags.CREATE
        | iflags.DELETE
        | iflags.MOVED_FROM
        | iflags.MOVED_TO
        | iflags.CLOSE_WRITE
        | iflags.ATTRIB
        | iflags.DELETE_SELF
        | iflags.MOVE_SELF
    )

# Normalized event types pushed onto the queue
EVT_CREATE = "create"
EVT_DELETE = "delete"
EVT_MODIFY = "modify"
EVT_MOVE_FROM = "move_from"
EVT_MOVE_TO = "move_to"


class InotifyWatcher(threading.Thread):
    """
    Daemon thread that drives an inotify watch over a directory tree.

    Pushes normalized event dicts to event_queue:
        {
            'type': str,        # EVT_* constant
            'path': str,        # absolute POSIX path
            'is_dir': bool,
            'cookie': int,      # non-zero only for move events (pairs MOVED_FROM/TO)
            'timestamp': float, # time.monotonic()
        }
    """

    def __init__(
        self,
        watch_root: str,
        efu_path: str,
        event_queue: queue.Queue,
        stop_event: threading.Event,
    ) -> None:
        super().__init__(name="InotifyWatcher", daemon=True)
        self._watch_root = watch_root
        self._efu_path = os.path.abspath(efu_path)
        self._event_queue = event_queue
        self._stop_event = stop_event

        if not INOTIFY_AVAILABLE:
            raise RuntimeError(
                "inotify_simple is not installed. "
                "Install it with: pip install inotify-simple"
            )

        self._inotify = inotify_simple.INotify()
        self._wd_to_path: dict[int, str] = {}
        self._path_to_wd: dict[str, int] = {}
        self._map_lock = threading.Lock()

    # ---------------------------------------------------------------- #
    # Watch tree management                                              #
    # ---------------------------------------------------------------- #

    def _add_watch(self, path: str) -> Optional[int]:
        try:
            wd = self._inotify.add_watch(path, _WATCH_FLAGS)
            with self._map_lock:
                self._wd_to_path[wd] = path
                self._path_to_wd[path] = wd
            return wd
        except PermissionError:
            log.warning("Permission denied adding watch: %s", path)
            return None
        except OSError as e:
            log.warning("Could not add watch for %s: %s", path, e)
            return None

    def _remove_watch(self, wd: int) -> None:
        try:
            self._inotify.rm_watch(wd)
        except OSError:
            pass  # Already removed by kernel (directory deleted)
        with self._map_lock:
            path = self._wd_to_path.pop(wd, None)
            if path:
                self._path_to_wd.pop(path, None)

    def _remove_watch_by_path(self, path: str) -> None:
        with self._map_lock:
            wd = self._path_to_wd.get(path)
        if wd is not None:
            self._remove_watch(wd)

    def _add_tree(self, root: str) -> None:
        """Recursively add inotify watches for root and all subdirectories."""
        self._add_watch(root)
        try:
            for dirpath, dirnames, _ in os.walk(root, followlinks=False):
                for dirname in dirnames:
                    full = os.path.join(dirpath, dirname)
                    self._add_watch(full)
        except PermissionError as e:
            log.warning("Permission error walking %s: %s", root, e)

    def _resolve_path(self, event) -> Optional[str]:
        """Map an inotify event to its absolute filesystem path."""
        with self._map_lock:
            dir_path = self._wd_to_path.get(event.wd)
        if dir_path is None:
            return None
        name = event.name  # bytes on some versions; decode if needed
        if isinstance(name, bytes):
            try:
                name = name.decode("utf-8", errors="replace")
            except Exception:
                return None
        if name:
            return os.path.join(dir_path, name)
        return dir_path

    # ---------------------------------------------------------------- #
    # Main loop                                                          #
    # ---------------------------------------------------------------- #

    def run(self) -> None:
        log.info("InotifyWatcher starting, adding watches for %s", self._watch_root)
        self._add_tree(self._watch_root)
        log.info("InotifyWatcher ready (%d watches)", len(self._wd_to_path))

        while not self._stop_event.is_set():
            try:
                events = self._inotify.read(timeout=1000)  # 1-second timeout
            except OSError as e:
                if self._stop_event.is_set():
                    break
                log.error("inotify read error: %s", e)
                continue

            for event in events:
                self._handle_event(event)

        log.info("InotifyWatcher stopped")
        try:
            self._inotify.close()
        except OSError:
            pass

    def _handle_event(self, event) -> None:
        path = self._resolve_path(event)
        if path is None:
            return

        # Skip the EFU file itself
        if path == self._efu_path:
            return

        mask = event.mask
        is_dir = bool(mask & iflags.ISDIR)
        timestamp = time.monotonic()

        if mask & iflags.CREATE:
            if is_dir:
                # New directory: add a watch subtree immediately to minimize the
                # race window where files inside could be missed
                self._add_tree(path)
            self._push(EVT_CREATE, path, is_dir, 0, timestamp)

        elif mask & iflags.MOVED_TO:
            if is_dir:
                self._add_tree(path)
            self._push(EVT_MOVE_TO, path, is_dir, event.cookie, timestamp)

        elif mask & iflags.MOVED_FROM:
            if is_dir:
                self._remove_watch_by_path(path)
            self._push(EVT_MOVE_FROM, path, is_dir, event.cookie, timestamp)

        elif mask & iflags.DELETE or mask & iflags.DELETE_SELF:
            if is_dir:
                self._remove_watch_by_path(path)
            self._push(EVT_DELETE, path, is_dir, 0, timestamp)

        elif mask & iflags.CLOSE_WRITE or mask & iflags.ATTRIB:
            if not is_dir:
                self._push(EVT_MODIFY, path, False, 0, timestamp)

        elif mask & iflags.MOVE_SELF:
            # The watched directory itself was moved — remove its watch
            with self._map_lock:
                wd = self._path_to_wd.get(path)
            if wd is not None:
                self._remove_watch(wd)

    def _push(
        self,
        evt_type: str,
        path: str,
        is_dir: bool,
        cookie: int,
        timestamp: float,
    ) -> None:
        self._event_queue.put_nowait(
            {
                "type": evt_type,
                "path": path,
                "is_dir": is_dir,
                "cookie": cookie,
                "timestamp": timestamp,
            }
        )
