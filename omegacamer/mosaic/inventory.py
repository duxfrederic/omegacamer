import os
from pathlib import Path
from astropy.io import fits
from database import Database
from logger import setup_logger

from utils import load_config, parse_filename, determine_night


def main():
    config = load_config(os.environ['OMEGACAMER_CONFIG'])
    work_dir = Path(config.get('mosaic_working_directory'))  # mandatory
    log_level = getattr(logging, config.get('logging', {}).get('level', 'INFO').upper(), logging.INFO)
    log_file = work_dir / config.get('logging', {}).get('file', 'omegacam_mosaic.log')
    logger = setup_logger(log_level, log_file)

    logger.info("Starting inventory process.")

    # init database
    db_name = config.get('database').get('name')  # mandatory for consistency
    db_path = work_dir / db_name
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
        fits_files = list(dir_path.glob(config['discovery_file_pattern']))
        logger.info(f"Found {len(fits_files)} FITS files in {dir_path}.")

        for fits_file in fits_files:
            try:
                timestamp, mjd, ccd_id = parse_filename(fits_file)
                target_name = fits.getheader(fits_file)['OBJECT']
                night_date = determine_night(timestamp)
                db.add_exposure(target_name=target_name,
                                night_date=night_date,
                                timestamp=timestamp,
                                mjd=mjd,
                                ccd_id=ccd_id,
                                file_path=str(fits_file))
            except ValueError as ve:
                logger.warning(str(ve))
            except Exception as e:
                logger.error(f"Error processing file {fits_file}: {e}")

    # check for epochs with missing CCDs
    incomplete_epochs = db.get_epochs_with_too_few_ccds()
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

