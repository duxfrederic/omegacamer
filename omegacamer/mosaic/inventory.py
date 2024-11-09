import os
import yaml
import sqlite3
from pathlib import Path
from datetime import datetime
from astropy.time import Time
from astropy.io import fits
from database import Database
from logger import setup_logger
import sys

def load_config(config_path):
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

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

def main():
    config = load_config(os.environ['OMEGACAMER_CONFIG'])
    log_level = getattr(logging, config.get('logging', {}).get('level', 'INFO').upper(), logging.INFO)
    log_file = config.get('logging', {}).get('file', 'omegacam_mosaic.log')
    logger = setup_logger(log_level, log_file)

    logger.info("Starting inventory process.")

    # init database
    db_name = config.get('database').get('name')  # mandatory for consistency
    work_dir = config.get('mosaic_working_directory')  # mandatory
    db_path = Path(work_dir) / db_name
    db = Database(db_path)
    logger.info(f"Connected to database at {db_path}.")

    # walk through each directory
    directories = config.get('directories', [])
    for dir_path in directories:
        dir_path = Path(dir_path)
        if not dir_path.exists() or not dir_path.is_dir():
            logger.error(f"Directory {dir_path} does not exist or is not a directory.")
            continue
        logger.info(f"Processing directory: {dir_path}")

        # list all fits files in there
        fits_files = list(dir_path.glob('OMEGA.*_OFCS.fits'))
        logger.info(f"Found {len(fits_files)} FITS files in {dir_path}.")

        for fits_file in fits_files:
            try:
                timestamp, mjd, ccd_id = parse_filename(fits_file)
                epoch_id = db.insert_epoch(timestamp, mjd)
                target = fits.getheader(fits_file)['OBJECT']
                db.insert_exposure(target, epoch_id, ccd_id, fits_file)
            except ValueError as ve:
                logger.warning(str(ve))
            except Exception as e:
                logger.error(f"Error processing file {fits_file}: {e}")

    # check for epochs with missing CCDs
    incomplete_epochs = db.get_epochs_with_ccd_count()
    if incomplete_epochs:
        logger.warning("The following epochs have missing CCDs:")
        for epoch in incomplete_epochs:
            logger.warning(f"Timestamp: {epoch[0]}, CCDs found: {epoch[1]}/32")
    else:
        logger.info("All epochs have complete CCD data.")

    db.close()
    logger.info("Inventory process completed.")

if __name__ == "__main__":
    import logging
    main()

