import sqlite3
from pathlib import Path

class Database:
    def __init__(self, db_path):
        self.db_path = Path(db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.create_tables()

    def create_tables(self):
        cursor = self.conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS epochs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                target TEXT,
                timestamp TEXT UNIQUE,
                mjd REAL
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS exposures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                epoch_id INTEGER,
                ccd_id INTEGER,
                file_path TEXT UNIQUE,
                FOREIGN KEY (epoch_id) REFERENCES epochs(id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mosaics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                target TEXT,
                night DATE,
                mosaic_file_path TEXT UNIQUE,
                UNIQUE(target, night)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mosaic_epochs (
                mosaic_id INTEGER,
                epoch_id INTEGER,
                FOREIGN KEY (mosaic_id) REFERENCES mosaics(id),
                FOREIGN KEY (epoch_id) REFERENCES epochs(id),
                PRIMARY KEY (mosaic_id, epoch_id)
            )
        """)

        self.conn.commit()

    def insert_epoch(self, target, timestamp, mjd):
        cursor = self.conn.cursor()
        try:
            cursor.execute("INSERT INTO epochs (target, timestamp, mjd) VALUES (?, ?, ?)", (target, timestamp, mjd))
            self.conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            # Epoch already exists
            cursor.execute("SELECT id FROM epochs WHERE timestamp = ?", (timestamp,))
            return cursor.fetchone()[0]

    def insert_exposure(self, epoch_id, ccd_id, file_path):
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO exposures (epoch_id, ccd_id, file_path)
                VALUES (?, ?, ?)
            """, (epoch_id, ccd_id, str(file_path)))
            self.conn.commit()
        except sqlite3.IntegrityError:
            # Exposure already exists
            pass

    def get_epochs_with_ccd_count(self, expected_count=32):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT epochs.timestamp, COUNT(exposures.id) as ccd_count
            FROM epochs
            JOIN exposures ON epochs.id = exposures.epoch_id
            GROUP BY epochs.id
            HAVING ccd_count != ?
        """, (expected_count,))
        return cursor.fetchall()

    def insert_mosaic(self, target, night, mosaic_file_path):
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO mosaics (target, night, mosaic_file_path)
                VALUES (?, ?, ?)
            """, (target, night, mosaic_file_path))
            self.conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            # Mosaic already exists
            cursor.execute("""
                SELECT id FROM mosaics WHERE target = ? AND night = ?
            """, (target, night))
            result = cursor.fetchone()
            return result[0] if result else None

    def get_mosaic(self, target, night):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT id, mosaic_file_path FROM mosaics
            WHERE target = ? AND night = ?
        """, (target, night))
        return cursor.fetchone()

    def associate_mosaic_epoch(self, mosaic_id, epoch_id):
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO mosaic_epochs (mosaic_id, epoch_id)
                VALUES (?, ?)
            """, (mosaic_id, epoch_id))
            self.conn.commit()
        except sqlite3.IntegrityError:
            # Association already exists
            pass
    def get_target_night_without_mosaic(self):
        """
        Retrieves groups (target, night) that do not have an associated mosaic.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT DISTINCT epochs.target, mosaics.night
            FROM epochs
            JOIN exposures ON epochs.id = exposures.epoch_id
            LEFT JOIN mosaic_epochs ON exposures.epoch_id = mosaic_epochs.epoch_id
            LEFT JOIN mosaics ON mosaic_epochs.mosaic_id = mosaics.id
            WHERE mosaics.id IS NULL
        """)
        return cursor.fetchall()

    def get_exposures_by_target_night(self, target, night):
        """
        Retrieves all exposures for a given target and night.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT exposures.id, exposures.file_path, exposures.ccd_id
            FROM exposures
            JOIN epochs ON exposures.epoch_id = epochs.id
            WHERE epochs.target = ? AND DATE(epochs.timestamp) = ?
        """, (target, night))
        return cursor.fetchall()

    def close(self):
        self.conn.close()

