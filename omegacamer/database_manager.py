import sqlite3
from pathlib import Path
import yaml


class DatabaseManager:
    def __init__(self, config_path='config.yaml'):
        self.config = self.load_config(config_path)
        self.working_directory = Path(self.config['working_directory'])
        self.db_path = self.working_directory / "downloads.sqlite3"
        self.conn = sqlite3.connect(self.db_path)
        self.create_downloads_table()
        self.create_calibrations_table()
        self.create_science_calibrations_table()

    def load_config(self, config_path):
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)

    def create_downloads_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS downloads (
            dp_id TEXT PRIMARY KEY,
            mjd_obs REAL,
            filter TEXT,
            airmass REAL,
            exptime REAL,
            type TEXT,
            file_path TEXT
        );
        """
        cursor = self.conn.cursor()
        cursor.execute(create_table_query)
        self.conn.commit()

    def create_calibrations_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS calibrations (
            calib_id TEXT PRIMARY KEY,
            calib_type TEXT,
            file_path TEXT
        );
        """
        cursor = self.conn.cursor()
        cursor.execute(create_table_query)
        self.conn.commit()

    def create_science_calibrations_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS science_calibrations (
            science_dp_id TEXT,
            calib_id TEXT,
            PRIMARY KEY (science_dp_id, calib_id),
            FOREIGN KEY (science_dp_id) REFERENCES downloads(dp_id),
            FOREIGN KEY (calib_id) REFERENCES calibrations(calib_id)
        );
        """
        cursor = self.conn.cursor()
        cursor.execute(create_table_query)
        self.conn.commit()

    # --- Science File Methods ---

    def record_exists(self, dp_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM downloads WHERE dp_id = ?", (dp_id,))
        return cursor.fetchone() is not None

    def infer_type(self, type_column, mode_column):
        if type_column.upper() == 'CALIB':
            mode = mode_column.lower()
            if 'flat' in mode:
                return 'flat'
            elif 'bias' in mode:
                return 'bias'
            elif 'dark' in mode:
                return 'dark'
            else:
                return 'calib'
        else:
            return type_column.lower()

    def add_science_record(self, record, working_directory):
        dp_id = record['Dataset ID']
        if self.record_exists(dp_id):
            print(f"Record with dp_id {dp_id} already exists. Skipping download.")
            return False  # Record already exists

        # Infer the type
        type_inferred = self.infer_type(record.get('Type', ''), record.get('Mode', ''))

        # Construct relative file path
        file_name = record.get('Filename', f"{dp_id}.fits")  # Adjust extension if necessary
        relative_path = Path(record.get('save_path', working_directory)) / file_name

        insert_query = """
        INSERT INTO downloads (dp_id, mjd_obs, filter, airmass, exptime, type, file_path)
        VALUES (?, ?, ?, ?, ?, ?, ?);
        """
        cursor = self.conn.cursor()
        cursor.execute(insert_query, (
            dp_id,
            record.get('MJD-OBS'),
            record.get('Filter'),
            record.get('Airmass'),
            record.get('Exptime'),
            type_inferred,
            str(relative_path.relative_to(working_directory))
        ))
        self.conn.commit()
        return True  # Record added

    # --- Calibration File Methods ---

    def calib_exists(self, calib_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM calibrations WHERE calib_id = ?", (calib_id,))
        return cursor.fetchone() is not None

    def add_calibration_record(self, calib_record):
        calib_id = calib_record['Dataset ID']
        if self.calib_exists(calib_id):
            print(f"Calibration record with calib_id {calib_id} already exists. Skipping.")
            return False  # Calibration already exists

        calib_type = self.infer_type(calib_record.get('Type', ''), calib_record.get('Mode', ''))

        # Construct relative file path
        file_name = calib_record.get('Filename', f"{calib_id}.fits")  # Adjust extension if necessary
        relative_path = Path(calib_record.get('save_path', self.working_directory)) / file_name

        insert_query = """
        INSERT INTO calibrations (calib_id, calib_type, file_path)
        VALUES (?, ?, ?);
        """
        cursor = self.conn.cursor()
        cursor.execute(insert_query, (
            calib_id,
            calib_type,
            str(relative_path.relative_to(self.working_directory))
        ))
        self.conn.commit()
        return True  # Calibration record added

    # --- Science-Calibrations Linking Methods ---

    def link_science_calibration(self, science_dp_id, calib_id):
        if not self.record_exists(science_dp_id):
            print(f"Science file {science_dp_id} does not exist in the database. Cannot link calibration.")
            return False

        if not self.calib_exists(calib_id):
            print(f"Calibration file {calib_id} does not exist in the database. Cannot link.")
            return False

        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO science_calibrations (science_dp_id, calib_id)
                VALUES (?, ?);
            """, (science_dp_id, calib_id))
            self.conn.commit()
            print(f"Linked calibration {calib_id} to science file {science_dp_id}.")
            return True
        except sqlite3.IntegrityError:
            print(f"Link between {science_dp_id} and {calib_id} already exists. Skipping.")
            return False  # Link already exists

    # --- Retrieval Methods ---

    def get_calibrations_for_science(self, science_dp_id):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT c.calib_id, c.calib_type, c.file_path
            FROM calibrations c
            JOIN science_calibrations sc ON c.calib_id = sc.calib_id
            WHERE sc.science_dp_id = ?
        """, (science_dp_id,))
        return cursor.fetchall()

    def get_sciences_for_calibration(self, calib_id):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT d.dp_id, d.type, d.file_path
            FROM downloads d
            JOIN science_calibrations sc ON d.dp_id = sc.science_dp_id
            WHERE sc.calib_id = ?
        """, (calib_id,))
        return cursor.fetchall()

    def close(self):
        self.conn.close()
