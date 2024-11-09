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
                target TEXT,
                FOREIGN KEY (epoch_id) REFERENCES epochs(id)
            )
        """)

        self.conn.commit()

    def insert_epoch(self, timestamp, mjd):
        cursor = self.conn.cursor()
        try:
            cursor.execute("INSERT INTO epochs (timestamp, mjd) VALUES (?, ?)", (timestamp, mjd))
            self.conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            # Epoch already exists
            cursor.execute("SELECT id FROM epochs WHERE timestamp = ?", (timestamp,))
            return cursor.fetchone()[0]

    def insert_exposure(self, target, epoch_id, ccd_id, file_path):
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO exposures (target, epoch_id, ccd_id, file_path)
                VALUES (?, ?, ?, ?)
            """, (target, epoch_id, ccd_id, str(file_path)))
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

    def close(self):
        self.conn.close()

