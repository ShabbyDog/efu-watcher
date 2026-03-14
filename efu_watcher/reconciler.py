"""
Startup reconciler for efu-watcher.

On startup, compares the SQLite index against the actual filesystem to catch
any changes that occurred while the daemon was offline, then rebuilds the EFU.
"""

import logging
import os
import stat
import time
from typing import TYPE_CHECKING

from efu_watcher.efu_writer import make_efu_line, posix_to_win_path, stat_to_row, filetime_to_posix

if TYPE_CHECKING:
    from efu_watcher.database import Database
    from efu_watcher.efu_writer import EfuWriter

log = logging.getLogger(__name__)

# Tolerance for mtime comparison (1 second) to account for filesystem timestamp
# precision differences between Linux stat and Windows FILETIME
_MTIME_TOLERANCE = 1.0


def _has_surrogates(path: str) -> bool:
    """
    Return True if path contains surrogate escape characters.

    os.walk() uses the surrogateescape error handler, so filenames with bytes
    that are not valid UTF-8 appear as strings containing lone surrogates
    (e.g. \\udc92). SQLite rejects these; Windows can't display them anyway.
    """
    try:
        path.encode("utf-8")
        return False
    except UnicodeEncodeError:
        return True


class Reconciler:
    def __init__(
        self,
        watch_root: str,
        db: "Database",
        efu_writer: "EfuWriter",
        win_prefix: str,
        exclude_paths: set[str],
    ) -> None:
        self._watch_root = watch_root
        self._db = db
        self._efu_writer = efu_writer
        self._win_prefix = win_prefix
        self._exclude = exclude_paths

    def run(self) -> None:
        """
        Full startup reconciliation:
          1. Scan disk
          2. Compare against DB
          3. Upsert new/changed, delete removed
          4. Rebuild EFU from DB
        """
        log.info("Reconciler starting full scan of %s", self._watch_root)
        t0 = time.monotonic()

        # If win_prefix changed since the last run, all cached efu_line values
        # are stale. Recompute them in the DB before doing anything else.
        stored_prefix = self._db.get_win_prefix()
        if stored_prefix is not None and stored_prefix != self._win_prefix:
            log.info(
                "win_prefix changed (%r -> %r); recomputing all efu_line values in DB",
                stored_prefix,
                self._win_prefix,
            )

            def _make_efu_line(path, size, date_modified, date_created, attributes):
                win_path = posix_to_win_path(path, self._watch_root, self._win_prefix)
                return make_efu_line(win_path, size, date_modified, date_created, attributes)

            n = self._db.recompute_efu_lines(self._watch_root, self._win_prefix, _make_efu_line)
            log.info("Recomputed efu_line for %d rows", n)
        elif stored_prefix is None:
            # First run — record the prefix so future changes are detected.
            self._db.set_win_prefix(self._win_prefix)

        disk_entries = self._scan_disk()
        log.info("Disk scan complete: %d entries in %.1fs", len(disk_entries), time.monotonic() - t0)

        db_paths = self._db.get_all_paths()
        log.info("DB has %d existing entries", len(db_paths))

        disk_paths = set(disk_entries.keys())
        new_paths = disk_paths - db_paths
        deleted_paths = db_paths - disk_paths
        common_paths = disk_paths & db_paths

        # Detect modified files among common paths
        modified_rows: list[dict] = []
        for path in common_paths:
            st = disk_entries[path]
            db_row = self._db.get_file(path)
            if db_row and self._is_modified(db_row, st):
                row = stat_to_row(path, st, self._watch_root, self._win_prefix)
                modified_rows.append(row)

        # Build rows for new paths
        new_rows = [
            stat_to_row(p, disk_entries[p], self._watch_root, self._win_prefix)
            for p in new_paths
        ]

        log.info(
            "Reconciliation: +%d new, -%d deleted, ~%d modified",
            len(new_rows),
            len(deleted_paths),
            len(modified_rows),
        )

        if new_rows:
            self._db.upsert_many(new_rows)
        if modified_rows:
            self._db.upsert_many(modified_rows)
        if deleted_paths:
            self._db.delete_many(list(deleted_paths))

        # Always persist the current prefix so future restarts can detect changes.
        self._db.set_win_prefix(self._win_prefix)

        log.info("DB sync complete — triggering EFU rebuild")
        self._efu_writer.full_rebuild(self._db)

        elapsed = time.monotonic() - t0
        log.info("Reconciler finished in %.1fs (DB now has %d entries)", elapsed, self._db.count())

    def _scan_disk(self) -> dict[str, os.stat_result]:
        """
        Walk the filesystem and return {abs_path: stat_result} for everything.
        Excludes paths in self._exclude.
        """
        entries: dict[str, os.stat_result] = {}
        count = 0

        skipped = 0
        for dirpath, dirnames, filenames in os.walk(self._watch_root, followlinks=False):
            # Stat and record the directory itself (unless it's the watch root)
            if dirpath != self._watch_root:
                if dirpath not in self._exclude:
                    if _has_surrogates(dirpath):
                        log.warning("Skipping directory with non-UTF-8 name: %r", dirpath)
                        skipped += 1
                        dirnames.clear()  # don't descend into it either
                        continue
                    try:
                        st = os.stat(dirpath)
                        entries[dirpath] = st
                        count += 1
                    except OSError:
                        pass

            # Stat each file
            for fname in filenames:
                full = os.path.join(dirpath, fname)
                if full in self._exclude:
                    continue
                if _has_surrogates(full):
                    log.warning("Skipping file with non-UTF-8 name: %r", full)
                    skipped += 1
                    continue
                try:
                    st = os.stat(full)
                    entries[full] = st
                    count += 1
                    if count % 100_000 == 0:
                        log.info("  Scanned %d entries...", count)
                except OSError:
                    pass

            # Prune excluded and non-UTF-8 directories from the walk
            dirnames[:] = [
                d for d in dirnames
                if os.path.join(dirpath, d) not in self._exclude
                and not _has_surrogates(os.path.join(dirpath, d))
            ]

        if skipped:
            log.warning("Skipped %d entries with non-UTF-8 names (not indexable on Windows)", skipped)

        return entries

    def _is_modified(self, db_row: dict, st: os.stat_result) -> bool:
        """
        Return True if the file appears modified compared to the DB entry.
        Compares mtime (with tolerance) and, for files, size.
        """
        db_mtime_posix = filetime_to_posix(db_row["date_modified"])
        if abs(st.st_mtime - db_mtime_posix) > _MTIME_TOLERANCE:
            return True
        if not stat.S_ISDIR(st.st_mode):
            if st.st_size != db_row["size"]:
                return True
        return False
