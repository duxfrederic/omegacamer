import sqlite3
from pathlib import Path


class Database:
    def __init__(self, db_path):
        self.db_path = Path(db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.create_tables()

    def __del__(self):
        self.conn.close()

    def create_tables(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS targets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nights (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT UNIQUE -- e.g. 2024-11-06, "night of the"
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS epochs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                target_id INTEGER,
                night_id INTEGER,
                timestamp TEXT UNIQUE,
                mjd REAL,
                FOREIGN KEY (target_id) REFERENCES targets(id),
                FOREIGN KEY (night_id) REFERENCES nights(id)
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
                target_id INTEGER,
                night_id INTEGER,
                mosaic_file_path TEXT UNIQUE,
                FOREIGN KEY (target_id) REFERENCES targets(id),
                FOREIGN KEY (night_id) REFERENCES nights(id)
            )
        """)

        self.conn.commit()

    def add_exposure(self, target_name, night_date, timestamp, mjd, ccd_id, file_path):
        cursor = self.conn.cursor()

        # add target if it doesn't exist
        cursor.execute("INSERT OR IGNORE INTO targets (name) VALUES (?)", (target_name,))
        # Add night if it doesn't exist
        cursor.execute("INSERT OR IGNORE INTO nights (date) VALUES (?)", (night_date,))

        # retrieve target_id and night_id
        cursor.execute("SELECT id FROM targets WHERE name = ?", (target_name,))
        target_id = cursor.fetchone()[0]
        cursor.execute("SELECT id FROM nights WHERE date = ?", (night_date,))
        night_id = cursor.fetchone()[0]

        # add epoch if it doesn't exist
        cursor.execute("""
            INSERT OR IGNORE INTO epochs (target_id, night_id, timestamp, mjd) 
            VALUES (?, ?, ?, ?)
        """, (target_id, night_id, timestamp, mjd))

        # get epoch_id back
        cursor.execute("SELECT id FROM epochs WHERE timestamp = ?", (timestamp,))
        epoch_id = cursor.fetchone()[0]

        # add exposure.
        cursor.execute("""
            INSERT OR IGNORE INTO exposures (epoch_id, ccd_id, file_path) 
            VALUES (?, ?, ?)
        """, (epoch_id, ccd_id, file_path))

        self.conn.commit()

    def get_missing_mosaics(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT nights.date, targets.name 
            FROM nights, targets
            LEFT JOIN mosaics ON mosaics.night_id = nights.id AND mosaics.target_id = targets.id
            WHERE mosaics.id IS NULL
        """)
        return cursor.fetchall()

    def add_mosaic(self, target_name, night_date, mosaic_file_path):
        cursor = self.conn.cursor()

        # Retrieve target_id and night_id
        cursor.execute("SELECT id FROM targets WHERE name = ?", (target_name,))
        target_id = cursor.fetchone()[0]
        cursor.execute("SELECT id FROM nights WHERE date = ?", (night_date,))
        night_id = cursor.fetchone()[0]

        # Add mosaic
        cursor.execute("""
            INSERT INTO mosaics (target_id, night_id, mosaic_file_path) 
            VALUES (?, ?, ?)
        """, (target_id, night_id, mosaic_file_path))

        self.conn.commit()

    def get_epochs_with_correct_ccd_count(self, expected_count=32):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT epochs.timestamp, COUNT(exposures.id) as ccd_count
            FROM epochs
            JOIN exposures ON epochs.id = exposures.epoch_id
            GROUP BY epochs.id
            HAVING ccd_count = ?
        """, (expected_count,))
        return cursor.fetchall()

    def get_epochs_with_too_few_ccds(self, expected_count=32):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT epochs.timestamp, COUNT(exposures.id) as ccd_count
            FROM epochs
            JOIN exposures ON epochs.id = exposures.epoch_id
            GROUP BY epochs.id
            HAVING ccd_count < ?
        """, (expected_count,))
        return cursor.fetchall()

    def get_exposures_for_mosaic(self, target_name, night_date):
        cursor = self.conn.cursor()

        # retrieve target_id and night_id
        cursor.execute("SELECT id FROM targets WHERE name = ?", (target_name,))
        target_id = cursor.fetchone()[0]
        cursor.execute("SELECT id FROM nights WHERE date = ?", (night_date,))
        night_id = cursor.fetchone()[0]

        # get all exposures for the given target and night
        cursor.execute("""
            SELECT exposures.file_path 
            FROM exposures
            JOIN epochs ON exposures.epoch_id = epochs.id
            WHERE epochs.target_id = ? AND epochs.night_id = ?
        """, (target_id, night_id))

        return [row[0] for row in cursor.fetchall()]

    def close(self):
        self.conn.close()
