"""
SQLite database layer for efu-watcher.

Each row represents one file or directory under the watch root.
The efu_line column caches the exact CSV line as it appears in the EFU file,
enabling fast in-place patches without fragile line-number tracking.
"""

import logging
import sqlite3
import threading
from datetime import datetime
from typing import Iterator, Optional

log = logging.getLogger(__name__)

_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS files (
    path          TEXT PRIMARY KEY,
    size          INTEGER NOT NULL,
    date_modified INTEGER NOT NULL,
    date_created  INTEGER NOT NULL,
    attributes    INTEGER NOT NULL DEFAULT 0,
    efu_line      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', '1');
"""


class Database:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._migrate()
        log.info("Database opened: %s", db_path)

    def _migrate(self) -> None:
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    # ------------------------------------------------------------------ #
    # Single-row operations                                                #
    # ------------------------------------------------------------------ #

    def upsert_file(
        self,
        path: str,
        size: int,
        date_modified: int,
        date_created: int,
        attributes: int,
        efu_line: str,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO files (path, size, date_modified, date_created, attributes, efu_line)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(path) DO UPDATE SET
                    size          = excluded.size,
                    date_modified = excluded.date_modified,
                    date_created  = excluded.date_created,
                    attributes    = excluded.attributes,
                    efu_line      = excluded.efu_line
                """,
                (path, size, date_modified, date_created, attributes, efu_line),
            )
            self._conn.commit()

    def delete_file(self, path: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM files WHERE path = ?", (path,))
            self._conn.commit()

    def get_file(self, path: str) -> Optional[dict]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM files WHERE path = ?", (path,)
            ).fetchone()
        return dict(row) if row else None

    def get_efu_line(self, path: str) -> Optional[str]:
        with self._lock:
            row = self._conn.execute(
                "SELECT efu_line FROM files WHERE path = ?", (path,)
            ).fetchone()
        return row["efu_line"] if row else None

    # ------------------------------------------------------------------ #
    # Bulk / directory operations                                          #
    # ------------------------------------------------------------------ #

    def get_children(self, dir_path: str) -> list[dict]:
        """Return all rows whose path starts with dir_path/ (non-recursive children)."""
        prefix = dir_path.rstrip("/") + "/"
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM files WHERE path LIKE ? ORDER BY path",
                (prefix + "%",),
            ).fetchall()
        return [dict(r) for r in rows]

    def rename_directory(
        self, old_prefix: str, new_prefix: str, make_efu_line_fn
    ) -> list[tuple[str, str]]:
        """
        Atomically rename all rows under old_prefix to new_prefix.

        make_efu_line_fn(path, size, date_modified, date_created, attributes) -> str
        is called to recompute efu_line for each affected row.

        Returns list of (old_efu_line, new_efu_line) pairs for EFU patching.
        """
        old_prefix = old_prefix.rstrip("/")
        new_prefix = new_prefix.rstrip("/")
        prefix_len = len(old_prefix)

        # Fetch directory row itself + all descendants
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM files WHERE path = ? OR path LIKE ?",
                (old_prefix, old_prefix + "/%"),
            ).fetchall()

            if not rows:
                log.warning("rename_directory: no rows found for %s", old_prefix)
                return []

            pairs: list[tuple[str, str]] = []
            updates: list[tuple[str, str, str, str]] = []  # (new_path, new_efu, old_efu, old_path)

            for row in rows:
                old_path = row["path"]
                new_path = new_prefix + old_path[prefix_len:]
                new_efu = make_efu_line_fn(
                    new_path,
                    row["size"],
                    row["date_modified"],
                    row["date_created"],
                    row["attributes"],
                )
                pairs.append((row["efu_line"], new_efu))
                updates.append((new_path, new_efu, row["efu_line"], old_path))

            self._conn.executemany(
                """
                UPDATE files
                SET path = ?, efu_line = ?
                WHERE efu_line = ? AND path = ?
                """,
                updates,
            )
            self._conn.commit()

        log.debug("rename_directory: %s -> %s (%d rows)", old_prefix, new_prefix, len(pairs))
        return pairs

    def upsert_many(self, rows: list[dict]) -> None:
        """Bulk upsert — used during reconciliation."""
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO files (path, size, date_modified, date_created, attributes, efu_line)
                VALUES (:path, :size, :date_modified, :date_created, :attributes, :efu_line)
                ON CONFLICT(path) DO UPDATE SET
                    size          = excluded.size,
                    date_modified = excluded.date_modified,
                    date_created  = excluded.date_created,
                    attributes    = excluded.attributes,
                    efu_line      = excluded.efu_line
                """,
                rows,
            )
            self._conn.commit()

    def delete_many(self, paths: list[str]) -> None:
        """Bulk delete — used during reconciliation."""
        with self._lock:
            self._conn.executemany(
                "DELETE FROM files WHERE path = ?",
                [(p,) for p in paths],
            )
            self._conn.commit()

    def delete_tree(self, dir_path: str) -> list[str]:
        """
        Delete a directory row and all descendant rows.
        Returns the list of deleted efu_lines for EFU patching.
        """
        prefix = dir_path.rstrip("/")
        with self._lock:
            rows = self._conn.execute(
                "SELECT efu_line FROM files WHERE path = ? OR path LIKE ?",
                (prefix, prefix + "/%"),
            ).fetchall()
            efu_lines = [r["efu_line"] for r in rows]
            self._conn.execute(
                "DELETE FROM files WHERE path = ? OR path LIKE ?",
                (prefix, prefix + "/%"),
            )
            self._conn.commit()
        return efu_lines

    def get_all_paths(self) -> set[str]:
        """Return the set of all indexed paths — used during reconciliation."""
        with self._lock:
            rows = self._conn.execute("SELECT path FROM files").fetchall()
        return {r["path"] for r in rows}

    def iter_all(self) -> Iterator[dict]:
        """
        Generator that yields every row — used during full EFU rebuild.
        Uses a separate connection to avoid blocking writes.
        """
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.execute("SELECT * FROM files ORDER BY path")
            while True:
                batch = cursor.fetchmany(1000)
                if not batch:
                    break
                for row in batch:
                    yield dict(row)
        finally:
            conn.close()

    def count(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) FROM files").fetchone()
        return row[0]

    # ------------------------------------------------------------------ #
    # Meta                                                                 #
    # ------------------------------------------------------------------ #

    def get_win_prefix(self) -> Optional[str]:
        """Return the win_prefix used to build the current efu_line values, or None."""
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key = 'win_prefix'"
            ).fetchone()
        return row["value"] if row else None

    def set_win_prefix(self, win_prefix: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO meta (key, value) VALUES ('win_prefix', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (win_prefix,),
            )
            self._conn.commit()

    def recompute_efu_lines(self, watch_root: str, win_prefix: str, make_row_fn) -> int:
        """
        Recompute efu_line for every row using the new win_prefix.

        make_row_fn(path, size, date_modified, date_created, attributes) -> efu_line str

        Returns the number of rows updated.
        """
        watch_root = watch_root.rstrip("/")
        count = 0
        with self._lock:
            cursor = self._conn.execute(
                "SELECT path, size, date_modified, date_created, attributes FROM files"
            )
            updates = []
            while True:
                batch = cursor.fetchmany(5000)
                if not batch:
                    break
                for row in batch:
                    new_efu_line = make_row_fn(
                        row["path"],
                        row["size"],
                        row["date_modified"],
                        row["date_created"],
                        row["attributes"],
                    )
                    updates.append((new_efu_line, row["path"]))
                count += len(batch)

            self._conn.executemany(
                "UPDATE files SET efu_line = ? WHERE path = ?", updates
            )
            self._conn.execute(
                "INSERT INTO meta (key, value) VALUES ('win_prefix', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (win_prefix,),
            )
            self._conn.commit()
        return count

    def get_last_rebuild(self) -> Optional[datetime]:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key = 'last_full_rebuild'"
            ).fetchone()
        if row is None:
            return None
        try:
            return datetime.fromisoformat(row["value"])
        except ValueError:
            return None

    def set_last_rebuild(self, dt: datetime) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO meta (key, value) VALUES ('last_full_rebuild', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (dt.isoformat(),),
            )
            self._conn.commit()

    def close(self) -> None:
        self._conn.close()
        log.debug("Database closed")
