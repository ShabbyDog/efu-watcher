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

from efu_watcher.efu_writer import stat_to_row, filetime_to_posix

if TYPE_CHECKING:
    from efu_watcher.database import Database
    from efu_watcher.efu_writer import EfuWriter

log = logging.getLogger(__name__)

# Tolerance for mtime comparison (1 second) to account for filesystem timestamp
# precision differences between Linux stat and Windows FILETIME
_MTIME_TOLERANCE = 1.0


class Reconciler:
    def __init__(
        self,
        watch_root: str,
        db: "Database",
        efu_writer: "EfuWriter",
        unc_host: str,
        unc_share: str,
        exclude_paths: set[str],
    ) -> None:
        self._watch_root = watch_root
        self._db = db
        self._efu_writer = efu_writer
        self._unc_host = unc_host
        self._unc_share = unc_share
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
                row = stat_to_row(path, st, self._watch_root, self._unc_host, self._unc_share)
                modified_rows.append(row)

        # Build rows for new paths
        new_rows = [
            stat_to_row(p, disk_entries[p], self._watch_root, self._unc_host, self._unc_share)
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

        for dirpath, dirnames, filenames in os.walk(self._watch_root, followlinks=False):
            # Stat and record the directory itself (unless it's the watch root)
            if dirpath != self._watch_root:
                if dirpath not in self._exclude:
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
                try:
                    st = os.stat(full)
                    entries[full] = st
                    count += 1
                    if count % 100_000 == 0:
                        log.info("  Scanned %d entries...", count)
                except OSError:
                    pass

            # Prune excluded directories from the walk
            dirnames[:] = [
                d for d in dirnames
                if os.path.join(dirpath, d) not in self._exclude
            ]

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
