"""
EFU file management for efu-watcher.

Handles:
  - EFU format utilities (FILETIME conversion, UNC path construction, CSV formatting)
  - Atomic full rebuild (temp file → rename)
  - In-place patching for single or batched line replacements
"""

import csv
import io
import logging
import os
import stat
import threading
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from efu_watcher.database import Database

log = logging.getLogger(__name__)

# Windows FILETIME epoch offset: 100-nanosecond ticks between 1601-01-01 and 1970-01-01
_EPOCH_DIFF = 116_444_736_000_000_000

# Windows file attribute flags
FILE_ATTRIBUTE_READONLY = 0x0001
FILE_ATTRIBUTE_HIDDEN = 0x0002
FILE_ATTRIBUTE_DIRECTORY = 0x0010
FILE_ATTRIBUTE_ARCHIVE = 0x0020

EFU_HEADER = "Filename,Size,Date Modified,Date Created,Attributes\n"


# ------------------------------------------------------------------ #
# Format utilities                                                     #
# ------------------------------------------------------------------ #


def posix_to_filetime(posix_timestamp: float) -> int:
    """Convert a POSIX timestamp (float seconds since 1970-01-01) to Windows FILETIME."""
    return int(posix_timestamp * 10_000_000) + _EPOCH_DIFF


def filetime_to_posix(filetime: int) -> float:
    """Convert a Windows FILETIME to a POSIX timestamp."""
    return (filetime - _EPOCH_DIFF) / 10_000_000


def attrs_from_stat(st: os.stat_result, name: str) -> int:
    """Compute Windows FILE_ATTRIBUTE_* flags from a stat result and filename."""
    flags = 0
    if stat.S_ISDIR(st.st_mode):
        flags |= FILE_ATTRIBUTE_DIRECTORY
    else:
        flags |= FILE_ATTRIBUTE_ARCHIVE
    if name.startswith("."):
        flags |= FILE_ATTRIBUTE_HIDDEN
    if not (st.st_mode & stat.S_IWUSR):
        flags |= FILE_ATTRIBUTE_READONLY
    return flags


def posix_to_unc(posix_path: str, watch_root: str, unc_host: str, unc_share: str) -> str:
    """
    Convert an absolute POSIX path under watch_root to a Windows UNC path.

    Example:
        /mnt/cube/Storage/foo/bar.txt  ->  \\\\cube.local\\Storage\\foo\\bar.txt
    """
    watch_root = watch_root.rstrip("/")
    rel = posix_path[len(watch_root):]  # '' for root itself, '/foo/bar' for children
    rel_win = rel.replace("/", "\\")
    return f"\\\\{unc_host}\\{unc_share}{rel_win}"


def make_efu_line(
    unc_path: str,
    size: int,
    date_modified: int,
    date_created: int,
    attributes: int,
) -> str:
    """
    Format a single EFU CSV line using the csv module for correct quoting.

    The returned string does NOT include a trailing newline.
    """
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="")
    writer.writerow([unc_path, size, date_modified, date_created, attributes])
    return buf.getvalue()


def stat_to_row(
    posix_path: str,
    st: os.stat_result,
    watch_root: str,
    unc_host: str,
    unc_share: str,
) -> dict:
    """
    Build a complete database row dict from a path and its stat result.
    """
    name = os.path.basename(posix_path)
    size = st.st_size if not stat.S_ISDIR(st.st_mode) else 0
    date_modified = posix_to_filetime(st.st_mtime)
    date_created = posix_to_filetime(getattr(st, "st_birthtime", st.st_ctime))
    attributes = attrs_from_stat(st, name)
    unc_path = posix_to_unc(posix_path, watch_root, unc_host, unc_share)
    efu_line = make_efu_line(unc_path, size, date_modified, date_created, attributes)

    return {
        "path": posix_path,
        "size": size,
        "date_modified": date_modified,
        "date_created": date_created,
        "attributes": attributes,
        "efu_line": efu_line,
    }


# ------------------------------------------------------------------ #
# EFU writer                                                           #
# ------------------------------------------------------------------ #


class EfuWriter:
    def __init__(self, efu_path: str, max_patch_size_mb: int = 200) -> None:
        self._efu_path = efu_path
        self._max_patch_bytes = max_patch_size_mb * 1024 * 1024
        self._lock = threading.Lock()

    # ---------------------------------------------------------------- #
    # Full rebuild                                                       #
    # ---------------------------------------------------------------- #

    def full_rebuild(self, db: "Database") -> None:
        """
        Write a fresh EFU from the database, atomically replacing the live file.
        Writes to a .tmp file first, then os.replace() for atomicity.
        """
        tmp_path = self._efu_path + ".tmp"
        count = 0

        log.info("Starting full EFU rebuild -> %s", tmp_path)
        try:
            with open(tmp_path, "w", encoding="utf-8", newline="") as f:
                f.write(EFU_HEADER)
                for row in db.iter_all():
                    f.write(row["efu_line"])
                    f.write("\n")
                    count += 1
                    if count % 100_000 == 0:
                        log.info("  ... %d records written", count)
                f.flush()
                os.fsync(f.fileno())

            os.replace(tmp_path, self._efu_path)
            log.info("Full EFU rebuild complete: %d records -> %s", count, self._efu_path)
        except Exception:
            log.exception("Full EFU rebuild failed; leaving live file untouched")
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def ensure_exists(self, db: "Database") -> None:
        """If the EFU file is missing or empty, trigger a full rebuild."""
        if not os.path.exists(self._efu_path) or os.path.getsize(self._efu_path) < len(EFU_HEADER):
            log.info("EFU file missing or empty — triggering full rebuild")
            self.full_rebuild(db)

    # ---------------------------------------------------------------- #
    # In-place patching                                                  #
    # ---------------------------------------------------------------- #

    def patch_line(self, old_efu_line: str, new_efu_line: Optional[str]) -> bool:
        """
        Replace or remove a single line in the EFU file.

        Returns True on success, False if old_efu_line was not found
        (caller should schedule a full rebuild).
        """
        return self.patch_many([(old_efu_line, new_efu_line)])

    def patch_many(self, replacements: list[tuple[str, Optional[str]]]) -> bool:
        """
        Apply a batch of line replacements/deletions in one read-write cycle.

        Each tuple is (old_efu_line, new_efu_line).
        new_efu_line=None means delete the line.

        Returns True if all old lines were found, False if any were missing.
        """
        if not replacements:
            return True

        with self._lock:
            if not os.path.exists(self._efu_path):
                log.warning("patch_many: EFU file does not exist")
                return False

            file_size = os.path.getsize(self._efu_path)
            if file_size > self._max_patch_bytes:
                log.warning(
                    "EFU file too large for patching (%d MB > %d MB limit); "
                    "need full rebuild",
                    file_size // (1024 * 1024),
                    self._max_patch_bytes // (1024 * 1024),
                )
                return False

            try:
                with open(self._efu_path, "r", encoding="utf-8") as f:
                    content = f.read()
            except OSError:
                log.exception("patch_many: failed to read EFU file")
                return False

            all_found = True
            for old_line, new_line in replacements:
                # Match the exact line including newline to prevent prefix collisions
                search_target = old_line + "\n"
                if search_target not in content:
                    log.warning("patch_many: line not found in EFU: %s", old_line[:80])
                    all_found = False
                    continue

                replacement = (new_line + "\n") if new_line is not None else ""
                content = content.replace(search_target, replacement, 1)

            tmp_path = self._efu_path + ".patch.tmp"
            try:
                with open(tmp_path, "w", encoding="utf-8", newline="") as f:
                    f.write(content)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_path, self._efu_path)
            except OSError:
                log.exception("patch_many: failed to write patched EFU")
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                return False

            return all_found

    def append_line(self, efu_line: str) -> bool:
        """
        Append a new line to the EFU file (more efficient than patch for pure creates).
        Returns False if the file doesn't exist yet.
        """
        with self._lock:
            if not os.path.exists(self._efu_path):
                return False
            try:
                with open(self._efu_path, "a", encoding="utf-8") as f:
                    f.write(efu_line + "\n")
                return True
            except OSError:
                log.exception("append_line: failed to append to EFU")
                return False
