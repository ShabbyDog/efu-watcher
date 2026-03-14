"""
efu-watcher daemon entry point.

Orchestrates:
  - Mount availability check with retry
  - Database and EFU writer initialization
  - Startup reconciliation (synchronous, before event processing begins)
  - InotifyWatcher thread (kernel starts buffering events before reconcile)
  - EventProcessor thread (debounces and applies filesystem events)
  - DailyRebuilder thread (full consistency rebuild at a configured hour)
  - Graceful shutdown on SIGTERM / SIGINT
"""

import logging
import os
import queue
import signal
import sys
import threading
import time
from datetime import datetime
from typing import Optional

from efu_watcher import (
    DAILY_REBUILD_HOUR,
    DB_PATH,
    DEBOUNCE_WINDOW,
    EFU_PATH,
    MAX_PATCH_SIZE_MB,
    UNC_HOST,
    UNC_SHARE,
    WATCH_ROOT,
)
from efu_watcher.database import Database
from efu_watcher.efu_writer import EfuWriter, has_surrogates, make_efu_line, posix_to_unc, stat_to_row
from efu_watcher.reconciler import Reconciler
from efu_watcher.watcher import (
    EVT_CREATE,
    EVT_DELETE,
    EVT_MODIFY,
    EVT_MOVE_FROM,
    EVT_MOVE_TO,
    InotifyWatcher,
)

log = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
# Mount helpers                                                        #
# ------------------------------------------------------------------ #


def check_mount(watch_root: str) -> bool:
    # os.path.ismount() only returns True for actual mountpoints, not for
    # subdirectories within a mount (e.g. /mnt/cube/Storage inside /mnt/cube).
    # Check existence and readability instead; that's what we actually need.
    return os.path.isdir(watch_root) and os.access(watch_root, os.R_OK)


def wait_for_mount(
    watch_root: str, stop_event: threading.Event, retry_interval: int = 30
) -> bool:
    if check_mount(watch_root):
        return True
    log.warning("%s is not available; waiting...", watch_root)
    while not stop_event.is_set():
        stop_event.wait(timeout=retry_interval)
        if check_mount(watch_root):
            log.info("%s is now mounted", watch_root)
            return True
    return False


# ------------------------------------------------------------------ #
# EventProcessor                                                       #
# ------------------------------------------------------------------ #


class EventProcessor(threading.Thread):
    """
    Consumes events from the watcher queue, debounces them, and applies
    changes to both the database and the EFU file.
    """

    def __init__(
        self,
        event_queue: queue.Queue,
        rebuild_queue: queue.Queue,
        db: Database,
        efu_writer: EfuWriter,
        stop_event: threading.Event,
        watch_root: str,
        unc_host: str,
        unc_share: str,
        exclude_paths: set[str],
        debounce_window: float = 3.0,
    ) -> None:
        super().__init__(name="EventProcessor", daemon=True)
        self._q = event_queue
        self._rebuild_q = rebuild_queue
        self._db = db
        self._efu = efu_writer
        self._stop = stop_event
        self._watch_root = watch_root
        self._unc_host = unc_host
        self._unc_share = unc_share
        self._exclude = exclude_paths
        self._debounce = debounce_window

    def run(self) -> None:
        log.info("EventProcessor started (debounce=%.1fs)", self._debounce)
        pending: list[dict] = []
        last_event_time: float = 0.0

        while not self._stop.is_set():
            try:
                event = self._q.get(timeout=0.5)
                pending.append(event)
                last_event_time = time.monotonic()
            except queue.Empty:
                pass

            # Drain any remaining queued events
            while not self._q.empty():
                try:
                    event = self._q.get_nowait()
                    pending.append(event)
                    last_event_time = time.monotonic()
                except queue.Empty:
                    break

            # Flush if debounce window has elapsed since last event
            if pending and (time.monotonic() - last_event_time) >= self._debounce:
                collapsed = self._collapse_events(pending)
                self._process_batch(collapsed)
                pending.clear()

        # Flush any remaining events on shutdown
        if pending:
            collapsed = self._collapse_events(pending)
            self._process_batch(collapsed)

        log.info("EventProcessor stopped")

    # ---------------------------------------------------------------- #
    # Event collapsing                                                   #
    # ---------------------------------------------------------------- #

    def _collapse_events(self, events: list[dict]) -> list[dict]:
        """
        Deduplicate and pair move events.

        Rules:
          - CREATE then DELETE for the same path → no-op (drop both)
          - Multiple MODIFY for the same path → keep latest
          - MODIFY then DELETE → keep only DELETE
          - MOVED_FROM + MOVED_TO with same cookie → synthesize 'rename'
          - MOVED_FROM without matching MOVED_TO → treat as DELETE
          - MOVED_TO without matching MOVED_FROM → treat as CREATE
        """
        # First pass: pair moves
        pending_from: dict[int, dict] = {}  # cookie -> move_from event
        paired_renames: list[dict] = []     # synthetic rename events
        non_move: list[dict] = []

        for evt in events:
            if evt["type"] == EVT_MOVE_FROM:
                pending_from[evt["cookie"]] = evt
            elif evt["type"] == EVT_MOVE_TO:
                cookie = evt["cookie"]
                if cookie in pending_from:
                    from_evt = pending_from.pop(cookie)
                    paired_renames.append({
                        "type": "rename",
                        "from_path": from_evt["path"],
                        "to_path": evt["path"],
                        "is_dir": evt["is_dir"],
                        "timestamp": evt["timestamp"],
                    })
                else:
                    # No matching from → treat as create
                    non_move.append({**evt, "type": EVT_CREATE})
            else:
                non_move.append(evt)

        # Unpaired MOVED_FROM → treat as delete
        for from_evt in pending_from.values():
            non_move.append({**from_evt, "type": EVT_DELETE})

        # Second pass: deduplicate non-move events by path
        seen: dict[str, dict] = {}  # path -> winning event
        for evt in non_move:
            path = evt["path"]
            if path not in seen:
                seen[path] = evt
                continue
            prev = seen[path]
            # DELETE wins over everything
            if evt["type"] == EVT_DELETE:
                seen[path] = evt
            # CREATE after DELETE → no-op (mark as skip)
            elif prev["type"] == EVT_DELETE and evt["type"] == EVT_CREATE:
                del seen[path]
            # Latest MODIFY wins
            elif evt["type"] == EVT_MODIFY and prev["type"] == EVT_MODIFY:
                if evt["timestamp"] > prev["timestamp"]:
                    seen[path] = evt

        return list(seen.values()) + paired_renames

    # ---------------------------------------------------------------- #
    # Batch processing                                                   #
    # ---------------------------------------------------------------- #

    def _process_batch(self, events: list[dict]) -> None:
        if not events:
            return

        log.debug("Processing batch of %d events", len(events))
        needs_rebuild = False

        for evt in events:
            try:
                if evt["type"] == "rename":
                    ok = self._handle_rename(evt["from_path"], evt["to_path"], evt["is_dir"])
                elif evt["type"] == EVT_CREATE:
                    self._handle_create(evt["path"], evt["is_dir"])
                    ok = True
                elif evt["type"] == EVT_DELETE:
                    ok = self._handle_delete(evt["path"], evt["is_dir"])
                elif evt["type"] == EVT_MODIFY:
                    ok = self._handle_modify(evt["path"])
                else:
                    ok = True

                if not ok:
                    needs_rebuild = True
            except Exception:
                log.exception("Error processing event %s", evt)
                needs_rebuild = True

        if needs_rebuild:
            try:
                self._rebuild_q.put_nowait("rebuild")
                log.info("Scheduled on-demand EFU rebuild due to patch failure")
            except queue.Full:
                pass  # Already one pending

    def _handle_create(self, path: str, is_dir: bool) -> None:
        if path in self._exclude:
            return
        if has_surrogates(path):
            log.warning("Skipping create event for non-UTF-8 path: %r", path)
            return
        try:
            st = os.stat(path)
        except FileNotFoundError:
            return  # Created and deleted before we processed it
        except OSError as e:
            log.warning("stat failed for created path %s: %s", path, e)
            return

        if is_dir:
            # Scan the new directory's contents to catch files created during
            # the race window between directory creation and watch setup
            rows = []
            for entry in self._scan_dir(path):
                rows.append(entry)
            if rows:
                self._db.upsert_many(rows)
                replacements = [(None, r["efu_line"]) for r in rows]
                # Append all new lines
                for r in rows:
                    self._efu.append_line(r["efu_line"])

        row = stat_to_row(path, st, self._watch_root, self._unc_host, self._unc_share)
        self._db.upsert_file(**row)
        self._efu.append_line(row["efu_line"])

    def _handle_delete(self, path: str, is_dir: bool) -> bool:
        if is_dir:
            efu_lines = self._db.delete_tree(path)
            if not efu_lines:
                return True
            ok = self._efu.patch_many([(line, None) for line in efu_lines])
        else:
            old_line = self._db.get_efu_line(path)
            self._db.delete_file(path)
            if old_line is None:
                return True
            ok = self._efu.patch_line(old_line, None)
        return ok

    def _handle_modify(self, path: str) -> bool:
        if path in self._exclude:
            return True
        if has_surrogates(path):
            log.warning("Skipping modify event for non-UTF-8 path: %r", path)
            return True
        old_line = self._db.get_efu_line(path)
        try:
            st = os.stat(path)
        except OSError:
            return True  # Deleted before we processed modify

        row = stat_to_row(path, st, self._watch_root, self._unc_host, self._unc_share)
        self._db.upsert_file(**row)

        if old_line is None:
            # File wasn't in index — treat as create
            return self._efu.append_line(row["efu_line"])

        if old_line == row["efu_line"]:
            return True  # Nothing changed in the EFU representation

        return self._efu.patch_line(old_line, row["efu_line"])

    def _handle_rename(self, from_path: str, to_path: str, is_dir: bool) -> bool:
        def _make_efu_line(path, size, date_modified, date_created, attributes):
            unc = posix_to_unc(path, self._watch_root, self._unc_host, self._unc_share)
            return make_efu_line(unc, size, date_modified, date_created, attributes)

        if is_dir:
            pairs = self._db.rename_directory(from_path, to_path, _make_efu_line)
            if not pairs:
                return True
            return self._efu.patch_many(pairs)
        else:
            old_line = self._db.get_efu_line(from_path)
            if old_line is None:
                # Source wasn't indexed — try to index the destination
                self._handle_create(to_path, False)
                return True

            try:
                st = os.stat(to_path)
            except OSError:
                self._db.delete_file(from_path)
                return self._efu.patch_line(old_line, None) if old_line else True

            row = stat_to_row(to_path, st, self._watch_root, self._unc_host, self._unc_share)
            self._db.delete_file(from_path)
            self._db.upsert_file(**row)
            return self._efu.patch_line(old_line, row["efu_line"])

    def _scan_dir(self, dir_path: str) -> list[dict]:
        """Recursively scan a directory and return rows for all contents."""
        rows = []
        try:
            for dirpath, dirnames, filenames in os.walk(dir_path, followlinks=False):
                for name in filenames:
                    full = os.path.join(dirpath, name)
                    if full in self._exclude:
                        continue
                    try:
                        st = os.stat(full)
                        rows.append(
                            stat_to_row(full, st, self._watch_root, self._unc_host, self._unc_share)
                        )
                    except OSError:
                        pass
                for name in dirnames:
                    full = os.path.join(dirpath, name)
                    try:
                        st = os.stat(full)
                        rows.append(
                            stat_to_row(full, st, self._watch_root, self._unc_host, self._unc_share)
                        )
                    except OSError:
                        pass
        except PermissionError:
            pass
        return rows


# ------------------------------------------------------------------ #
# DailyRebuilder                                                       #
# ------------------------------------------------------------------ #


class DailyRebuilder(threading.Thread):
    """
    Performs a full EFU rebuild once per day at a configured hour,
    and also responds to on-demand rebuild signals from EventProcessor.
    """

    def __init__(
        self,
        db: Database,
        efu_writer: EfuWriter,
        stop_event: threading.Event,
        rebuild_queue: queue.Queue,
        rebuild_hour: int = 3,
    ) -> None:
        super().__init__(name="DailyRebuilder", daemon=True)
        self._db = db
        self._efu = efu_writer
        self._stop = stop_event
        self._rebuild_q = rebuild_queue
        self._rebuild_hour = rebuild_hour

    def _seconds_until_next_rebuild(self) -> float:
        now = datetime.now()
        target = now.replace(hour=self._rebuild_hour, minute=0, second=0, microsecond=0)
        if target <= now:
            target = target.replace(day=target.day + 1)
        return (target - now).total_seconds()

    def run(self) -> None:
        log.info("DailyRebuilder started (daily at %02d:00)", self._rebuild_hour)
        while not self._stop.is_set():
            # Wait for either the daily timer or an on-demand signal
            wait_secs = self._seconds_until_next_rebuild()
            log.info("Next full rebuild in %.0f seconds", wait_secs)

            deadline = time.monotonic() + wait_secs
            while not self._stop.is_set():
                remaining = deadline - time.monotonic()
                try:
                    self._rebuild_q.get(timeout=min(remaining, 60))
                    log.info("On-demand rebuild triggered")
                    break
                except queue.Empty:
                    if time.monotonic() >= deadline:
                        log.info("Scheduled daily rebuild triggered")
                        break

            if self._stop.is_set():
                break

            try:
                self._efu.full_rebuild(self._db)
                self._db.set_last_rebuild(datetime.now())
            except Exception:
                log.exception("Full rebuild failed")

        log.info("DailyRebuilder stopped")


# ------------------------------------------------------------------ #
# Entry point                                                          #
# ------------------------------------------------------------------ #


def setup_logging() -> None:
    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        stream=sys.stdout,
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


def main() -> int:
    setup_logging()

    # Configuration from environment (with defaults from __init__.py)
    watch_root = os.environ.get("WATCH_ROOT", WATCH_ROOT)
    efu_path = os.environ.get("EFU_PATH", EFU_PATH)
    db_path = os.environ.get("DB_PATH", DB_PATH)
    unc_host = os.environ.get("UNC_HOST", UNC_HOST)
    unc_share = os.environ.get("UNC_SHARE", UNC_SHARE)
    debounce = float(os.environ.get("DEBOUNCE_WINDOW", DEBOUNCE_WINDOW))
    rebuild_hour = int(os.environ.get("DAILY_REBUILD_HOUR", DAILY_REBUILD_HOUR))
    max_patch_mb = int(os.environ.get("MAX_PATCH_SIZE_MB", MAX_PATCH_SIZE_MB))

    log.info("efu-watcher starting")
    log.info("  WATCH_ROOT=%s  EFU_PATH=%s  DB_PATH=%s", watch_root, efu_path, db_path)
    log.info("  UNC=\\\\%s\\%s  DEBOUNCE=%.1fs  REBUILD_HOUR=%d", unc_host, unc_share, debounce, rebuild_hour)

    stop_event = threading.Event()

    def _handle_signal(signum, frame):
        log.info("Received signal %d — shutting down", signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    # Wait for the mount to be available before doing anything else
    if not wait_for_mount(watch_root, stop_event):
        log.warning("Shutdown requested while waiting for mount")
        return 0

    # Ensure DB directory exists
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)

    db = Database(db_path)
    efu_writer = EfuWriter(efu_path, max_patch_size_mb=max_patch_mb)
    exclude_paths = {os.path.abspath(efu_path)}

    event_queue: queue.Queue = queue.Queue()
    rebuild_queue: queue.Queue = queue.Queue(maxsize=1)

    # Step 1: Start inotify watcher so the kernel begins buffering events NOW,
    # before the reconciler scan, to avoid missing events during reconciliation.
    watcher = InotifyWatcher(watch_root, efu_path, event_queue, stop_event)
    watcher.start()

    # Step 2: Run reconciler synchronously — catches any offline changes.
    reconciler = Reconciler(
        watch_root=watch_root,
        db=db,
        efu_writer=efu_writer,
        unc_host=unc_host,
        unc_share=unc_share,
        exclude_paths=exclude_paths,
    )
    try:
        reconciler.run()
    except Exception:
        log.exception("Reconciler failed — continuing with potentially stale index")

    # Step 3: Start event processing threads
    processor = EventProcessor(
        event_queue=event_queue,
        rebuild_queue=rebuild_queue,
        db=db,
        efu_writer=efu_writer,
        stop_event=stop_event,
        watch_root=watch_root,
        unc_host=unc_host,
        unc_share=unc_share,
        exclude_paths=exclude_paths,
        debounce_window=debounce,
    )

    rebuilder = DailyRebuilder(
        db=db,
        efu_writer=efu_writer,
        stop_event=stop_event,
        rebuild_queue=rebuild_queue,
        rebuild_hour=rebuild_hour,
    )

    processor.start()
    rebuilder.start()

    log.info("efu-watcher running")

    # Block main thread until shutdown signal
    stop_event.wait()

    log.info("Shutting down threads...")
    watcher.join(timeout=10)
    processor.join(timeout=30)
    rebuilder.join(timeout=10)

    db.close()
    log.info("efu-watcher stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
