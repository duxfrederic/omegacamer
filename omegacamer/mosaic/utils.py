from datetime import datetime, timedelta
import pytz
import yaml
from astropy.time import Time
import shutil
from pathlib import Path
import importlib.resources as pkg_resources


def parse_filename(filename):
    """
    Parses the filename to extract timestamp and CCD ID.
    Expected format: OMEGA.<timestamp>_<ccd_id>OFCS.fits
    Example: OMEGA.2024-11-08T06:33:23.138_10OFCS.fits
    """
    try:
        base = filename.stem  # remove .fits
        parts = base.split('_')
        timestamp_str = parts[0].split('.')[1]  # e.g. '2024-11-08T06:33:23.138'
        ccd_id_str = parts[1].replace('OFCS', '')  # e.g. '10'
        timestamp = timestamp_str
        # to MJD
        t = Time(timestamp, format='isot', scale='utc')
        mjd = t.mjd
        ccd_id = int(ccd_id_str)
        return timestamp, mjd, ccd_id
    except Exception as e:
        raise ValueError(f"Filename {filename} does not match expected format.") from e


def load_config(config_path):
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def determine_night(timestamp_utc, timezone='America/Santiago'):
    """
    Determines the night date based on Paranal (Chile) local time.
    Night is defined as from 8 PM to 7 AM local time.

    Parameters:
    - timestamp_utc (str): Timestamp in (to the second) ISO format (e.g., '2024-11-08T06:33:23').
    - timezone (str): Timezone string (default: 'America/Santiago').

    Returns:
    - night_date (str): Date string in 'YYYY-MM-DD' format representing the night.
    """
    tz = pytz.timezone(timezone)
    dt_utc = datetime.strptime(timestamp_utc, "%Y-%m-%dT%H:%M:%S")
    dt_local = pytz.utc.localize(dt_utc).astimezone(tz)

    if dt_local.hour >= 20:
        night_date = dt_local.date()
    else:
        night_date = (dt_local - timedelta(days=1)).date()

    return night_date.isoformat()


def copy_static_configs(destination: Path, files: list):
    """
    Copy static configuration files bundled with the package to the destination directory.

    :param destination: Path to the destination directory.
    :param files: List of file names to copy.
    """
    from omegacamer.mosaic import static_configs

    for file_name in files:
        try:
            src_file = pkg_resources.files(static_configs) / file_name
            dest_file = destination / file_name
            shutil.copy(src_file, dest_file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Static configuration file not found: {file_name}")

