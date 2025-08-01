from __future__ import annotations
from pathlib import Path
from typing import Iterable, Sequence
import hashlib
import sqlite3
import yaml
from datetime import datetime, timezone



def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def md5(path: str, blocksize: int = 2 ** 20) -> str:
    """Cheap hash of the saved files."""
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(blocksize), b""):
            h.update(chunk)
    return h.hexdigest()



class DatabaseManager:
    """Book‑keeping for our OmegaCAM pipeline."""

    SCHEMA_VERSION = 1

    def __init__(self, config_path: str | Path = "config.yaml") -> None:
        with open(config_path, "r", encoding="utf‑8") as fh:
            cfg = yaml.safe_load(fh)

        self.working_directory = Path(cfg["working_directory"]).expanduser().resolve()
        self.working_directory.mkdir(parents=True, exist_ok=True)
        self.db_path = self.working_directory / "book_keeping.sqlite3"
        existed = self.db_path.exists()
        self.conn = sqlite3.connect(self.db_path)
        # rows come back as dict‑like objects, nicer to work with
        self.conn.row_factory = sqlite3.Row
        # enforce referential integrity
        self.conn.execute("PRAGMA foreign_keys = ON;")

        if not existed:
            self._create_schema()
        else:
            self._migrate_if_necessary()

    def _migrate_if_necessary(self) -> None:  # pragma: no cover
        cur = self.conn.cursor()
        row = cur.execute("SELECT value FROM meta WHERE key = 'schema_version';").fetchone()
        if row is None:
            raise RuntimeError("Database missing schema_version, cannot determine upgrade path.")
        version = int(row["value"])
        if version != self.SCHEMA_VERSION:
            raise NotImplementedError(
                f"Automatic migration path from version {version} to {self.SCHEMA_VERSION} not implemented."
            )

    def _create_schema(self) -> None:
        cur = self.conn.cursor()

        # raw science frames
        cur.execute(
            """
            CREATE TABLE raw_science_files (
                dp_id           TEXT PRIMARY KEY,
                object          TEXT,
                mjd_obs         REAL,
                filter          TEXT,
                binning         TEXT,
                readout_mode    TEXT,
                exptime         REAL,
                file_path       TEXT NOT NULL,
                checksum        TEXT NOT NULL,
                downloaded_at   TEXT NOT NULL DEFAULT (datetime('now'))
            );
            """
        )
        cur.execute("CREATE INDEX idx_science_lookup ON raw_science_files(filter, binning, readout_mode, mjd_obs);")

        # raw calibration frames
        cur.execute(
            """
            CREATE TABLE biases (
                calib_id        TEXT PRIMARY KEY,
                mjd_obs         REAL,
                binning         TEXT,
                readout_mode    TEXT,
                file_path       TEXT NOT NULL,
                checksum        TEXT NOT NULL,
                downloaded_at   TEXT NOT NULL DEFAULT (datetime('now'))
            );
            """
        )
        cur.execute("CREATE INDEX idx_bias_lookup ON biases(binning, readout_mode, mjd_obs);")

        cur.execute(
            """
            CREATE TABLE flats (
                calib_id        TEXT PRIMARY KEY,
                mjd_obs         REAL NOT NULL,
                filter          TEXT NOT NULL,
                type            TEXT, -- sky or dome
                binning         TEXT NOT NULL,
                readout_mode    TEXT NOT NULL,
                file_path       TEXT NOT NULL,
                checksum        TEXT NOT NULL,
                downloaded_at   TEXT NOT NULL DEFAULT (datetime('now'))
            );
            """
        )
        cur.execute("CREATE INDEX idx_flat_lookup ON flats(filter, binning, readout_mode, mjd_obs);")

        # unused calibratiosn
        cur.execute(
            """
            CREATE TABLE unused_calibrations (
                calib_id        TEXT PRIMARY KEY,
                type            TEXT NOT NULL
            );
            """
        )

        # combined calibrations
        cur.execute(
            """
            CREATE TABLE combined_biases (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                binning         TEXT,
                readout_mode    TEXT,
                file_path       TEXT NOT NULL,
                average_mjd_obs REAL,
                scatter_mjd_obs REAL,
                created_at      TEXT NOT NULL,
                UNIQUE(binning, readout_mode, average_mjd_obs)
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE combined_flats (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                binning         TEXT,
                readout_mode    TEXT,
                filter          TEXT,
                combined_bias   INTEGER REFERENCES combined_biases(id) ON UPDATE CASCADE ON DELETE SET NULL,
                file_path       TEXT NOT NULL,
                average_mjd_obs REAL,
                scatter_mjd_obs REAL,
                created_at      TEXT NOT NULL,
                UNIQUE(filter, binning, readout_mode, average_mjd_obs)
            );
            """
        )

        # track what went into what calib
        cur.execute(
            """
            CREATE TABLE combined_bias_members (
                combined_bias   INTEGER REFERENCES combined_biases(id) ON DELETE CASCADE,
                bias_calib_id   TEXT    REFERENCES biases(calib_id)   ON DELETE CASCADE,
                PRIMARY KEY (combined_bias, bias_calib_id)
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE combined_flat_members (
                combined_flat   INTEGER REFERENCES combined_flats(id) ON DELETE CASCADE,
                flat_calib_id   TEXT    REFERENCES flats(calib_id)    ON DELETE CASCADE,
                PRIMARY KEY (combined_flat, flat_calib_id)
            );
            """
        )

        # reduced files
        cur.execute(
            """
            CREATE TABLE reduced_science_files (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_dp_id           TEXT UNIQUE REFERENCES raw_science_files(dp_id) ON DELETE CASCADE,
                combined_bias       INTEGER REFERENCES combined_biases(id),
                combined_flat       INTEGER REFERENCES combined_flats(id),
                file_path           TEXT NOT NULL,
                checksum            TEXT NOT NULL,
                processed_at        TEXT NOT NULL,
                processing_version  TEXT
            );
            """
        )

        # misc
        cur.execute(
            """
            CREATE TABLE meta (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
            """
        )
        cur.execute("INSERT INTO meta(key, value) VALUES ('schema_version', ?);", (self.SCHEMA_VERSION,))

        self.conn.commit()


    def register_raw_science(self, *, dp_id: str, object_: str, mjd_obs: float, filter_: str, binning: str,
                              readout_mode: str, exptime: float, path: Path) -> None:
        checksum = md5(path)
        self.conn.execute(
            """
            INSERT INTO raw_science_files (dp_id, object, mjd_obs, filter, binning, readout_mode,
                                           exptime, file_path, checksum, downloaded_at)
            VALUES (:dp_id, :object, :mjd, :filter, :binning, :ro, :exptime, :fp, :ck, :ts)
            ON CONFLICT(dp_id) DO NOTHING;
            """,
            {
                "dp_id": dp_id,
                "object": object_,
                "mjd": mjd_obs,
                "filter": filter_,
                "binning": binning,
                "ro": readout_mode,
                "exptime": exptime,
                "fp": str(path),
                "ck": checksum,
                "ts": utc_now(),
            },
        )
        self.conn.commit()

    def register_bias(self, *, calib_id: str, mjd_obs: float, binning: str,
                      readout_mode: str, path: Path) -> None:
        checksum = md5(path)
        self.conn.execute(
            """
            INSERT INTO biases (calib_id, mjd_obs, binning, readout_mode, file_path, checksum, downloaded_at)
            VALUES (:id, :mjd, :bin, :ro, :fp, :ck, :ts)
            ON CONFLICT(calib_id) DO NOTHING;
            """,
            {"id": calib_id, "mjd": mjd_obs, "bin": binning, "ro": readout_mode,
                       "fp": str(path), "ck": checksum, "ts": utc_now()},
        )
        self.conn.commit()

    def register_flat(self, *, calib_id: str, mjd_obs: float, filter_: str, binning: str, type_: str,
                      readout_mode: str, path: Path) -> None:
        checksum = md5(path)
        self.conn.execute(
            """
            INSERT INTO flats (calib_id, mjd_obs, filter, type, binning, readout_mode, file_path, checksum, downloaded_at)
            VALUES (:id, :mjd, :flt, :tp, :bin, :ro, :fp, :ck, :ts)
            ON CONFLICT(calib_id) DO NOTHING;
            """,
            {"id": calib_id, "mjd": mjd_obs, "flt": filter_, "tp": type_, "bin": binning,
                       "ro": readout_mode, "fp": str(path), "ck": checksum, "ts": utc_now()},
        )
        self.conn.commit()

    def register_unused_calib(self, *, calib_id: str, type_: str) -> None:
        self.conn.execute(
            """
            INSERT INTO unused_calibrations (calib_id, type)
            VALUES (:id, :type)
            ON CONFLICT(calib_id) DO NOTHING;
            """,
            {"id": calib_id, "type": type_,},
        )
        self.conn.commit()


    # lookup helpers

    def find_biases(self, *, binning: str, readout_mode: str, mjd_window: float, mjd_center: float) -> Sequence[sqlite3.Row]:
        """Return bias frames matching *binning* & *readout_mode* whose mjd_obs is within
        ±*mjd_window* days of *mjd_center*.
        """
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT * FROM biases
             WHERE binning = :bin AND readout_mode = :ro
               AND ABS(mjd_obs - :mjd) <= :w
             ORDER BY ABS(mjd_obs - :mjd);
            """,
            {"bin": binning, "ro": readout_mode, "mjd": mjd_center, "w": mjd_window},
        )
        return cur.fetchall()

    def find_flats(self, *, filter_: str, binning: str, readout_mode: str, type_: str,
                   mjd_window: float, mjd_center: float) -> Sequence[sqlite3.Row]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT * FROM flats
             WHERE filter = :flt AND binning = :bin AND readout_mode = :ro AND type = :type
               AND ABS(mjd_obs - :mjd) <= :w
             ORDER BY ABS(mjd_obs - :mjd);
            """,
            {"flt": filter_, "bin": binning, "ro": readout_mode, "type": type_,
                       "mjd": mjd_center, "w": mjd_window},
        )
        return cur.fetchall()

    # combined calibration + reduced product registration
    def register_combined_bias(
        self,
        *,
        binning: str,
        readout_mode: str,
        member_calib_ids: Iterable[str],  # raw bias calib_ids
        output_path: Path,
        avg_mjd: float,
        scatter_mjd: float,
    ) -> int:
        """Insert a new combined bias and return its DB *id*. If an identical one (by
        UNIQUE constraint) already exists we reuse it.
        """
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO combined_biases (binning, readout_mode, file_path, average_mjd_obs, scatter_mjd_obs, created_at)
            VALUES (:bin, :ro, :fp, :avg, :scat, :ts)
            ON CONFLICT(binning, readout_mode, average_mjd_obs) DO UPDATE
              SET scatter_mjd_obs = excluded.scatter_mjd_obs
            RETURNING id;
            """,
            {"bin": binning, "ro": readout_mode, "fp": str(output_path), "avg": avg_mjd, "scat": scatter_mjd, "ts": utc_now()},
        )
        bias_id = cur.fetchone()[0]

        # provenance: many‑to‑one
        cur.executemany(
            """
            INSERT OR IGNORE INTO combined_bias_members (combined_bias, bias_calib_id)
            VALUES (:cb, :bid);
            """,
            [{"cb": bias_id, "bid": b} for b in member_calib_ids],
        )
        self.conn.commit()
        return bias_id

    def register_combined_flat(
        self,
        *,
        filter_: str,
        binning: str,
        readout_mode: str,
        combined_bias_id: int | None,  # may be NULL if debias skipped
        member_calib_ids: Iterable[str],
        output_path: Path,
        avg_mjd: float,
        scatter_mjd: float,
    ) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO combined_flats (filter, binning, readout_mode, combined_bias, file_path,
                                        average_mjd_obs, scatter_mjd_obs, created_at)
            VALUES (:flt, :bin, :ro, :cb, :fp, :avg, :scat, :ts)
            ON CONFLICT(filter, binning, readout_mode, average_mjd_obs) DO UPDATE
              SET scatter_mjd_obs = excluded.scatter_mjd_obs
            RETURNING id;
            """,
            {
                "flt": filter_,
                "bin": binning,
                "ro": readout_mode,
                "cb": combined_bias_id,
                "fp": str(output_path),
                "avg": avg_mjd,
                "scat": scatter_mjd,
                "ts": utc_now(),
            },
        )
        flat_id = cur.fetchone()[0]

        cur.executemany(
            """
            INSERT OR IGNORE INTO combined_flat_members (combined_flat, flat_calib_id)
            VALUES (:cf, :fid);
            """,
            [{"cf": flat_id, "fid": f} for f in member_calib_ids],
        )
        self.conn.commit()
        return flat_id

    def register_reduced_science(
        self,
        *,
        raw_dp_id: str,
        combined_bias_id: int | None,
        combined_flat_id: int | None,
        output_path: Path,
        processing_version: str,
    ) -> None:
        checksum = md5(output_path)
        self.conn.execute(
            """
            INSERT INTO reduced_science_files (raw_dp_id, combined_bias, combined_flat, file_path,
                                               checksum, processed_at, processing_version)
            VALUES (:dp, :cb, :cf, :fp, :ck, :ts, :ver)
            ON CONFLICT(raw_dp_id) DO UPDATE
              SET file_path = excluded.file_path,
                  checksum  = excluded.checksum,
                  processed_at = excluded.processed_at,
                  processing_version = excluded.processing_version;
            """,
            {
                "dp": raw_dp_id,
                "cb": combined_bias_id,
                "cf": combined_flat_id,
                "fp": str(output_path),
                "ck": checksum,
                "ts": utc_now(),
                "ver": processing_version,
            },
        )
        self.conn.commit()
        
        
    # checkers
    def raw_science_exists(self, dp_id: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM raw_science_files WHERE dp_id = ? LIMIT 1;",
            (dp_id,)
        ).fetchone()
        return row is not None

    def bias_exists(self, calib_id: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM biases WHERE calib_id = ? LIMIT 1;",
            (calib_id,)
        ).fetchone()
        return row is not None

    def flat_exists(self, calib_id: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM flats WHERE calib_id = ? LIMIT 1;",
            (calib_id,)
        ).fetchone()
        return row is not None

    def unusedcalib_exists(self, calib_id: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM unused_calibrations WHERE calib_id = ? LIMIT 1;",
            (calib_id,)
        ).fetchone()
        return row is not None

    def reduced_science_exists(self, dp_id: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM reduced_science_files WHERE raw_dp_id = ? LIMIT 1;",
            (dp_id,)
        ).fetchone()
        return row is not None

    def combined_bias_exists(self, *, binning: str, readout_mode: str, avg_mjd: float) -> bool:
        row = self.conn.execute(
        """        
        SELECT 1 FROM combined_biases
                WHERE binning = ? AND readout_mode = ? AND average_mjd_obs = ?
                LIMIT 1;
        """,
            (binning, readout_mode, avg_mjd),
        ).fetchone()
        return row is not None

    def combined_flat_exists(self, *, filter_: str, binning: str, readout_mode: str, avg_mjd: float) -> bool:
        row = self.conn.execute(
        """ 
        SELECT 1 FROM combined_flats
                WHERE filter = ? AND binning = ? AND readout_mode = ? AND average_mjd_obs = ?
                LIMIT 1;
        """,
        (filter_, binning, readout_mode, avg_mjd),
        ).fetchone()
        return row is not None

    def close(self) -> None:
        self.conn.close()
