from pathlib import Path
from database import Database
from logger import setup_logger
import logging
import os
import sys

from utils import determine_night, load_config


def main():
    config_path = os.environ.get('OMEGACAMER_CONFIG')
    if not config_path:
        print("Environment variable 'OMEGACAMER_CONFIG' not set.")
        sys.exit(1)

    config = load_config(config_path)

    work_dir = Path(config.get('mosaic_working_directory'))
    work_dir.mkdir(parents=True, exist_ok=True)  # Ensure work_dir exists

    log_level_str = config.get('logging', {}).get('level', 'INFO').upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    log_file = work_dir / config.get('logging', {}).get('file', 'omegacam_mosaic.log')
    logger = setup_logger(log_level, log_file)

    logger.info("Starting grouping and linking process.")

    db_name = config.get('database').get('name')  # Mandatory for consistency
    db_path = work_dir / db_name
    db = Database(db_path)
    logger.info(f"Connected to database at {db_path}.")

    # fetch all epochs from the database
    cursor = db.conn.cursor()
    cursor.execute("SELECT id, target, timestamp FROM epochs")
    epochs = cursor.fetchall()

    # group epochs by target and night
    grouped = {}
    for epoch in epochs:
        epoch_id, target, timestamp = epoch
        night = determine_night(timestamp)
        key = (target, night)
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(epoch_id)

    logger.info(f"Grouped into {len(grouped)} target-night combinations.")

    # process each group
    for (target, night), epoch_ids in grouped.items():
        logger.info(f"Processing Target: {target}, Night: {night}")

        # check if mosaic already exists
        existing_mosaic = db.get_mosaic(target, night)
        if existing_mosaic:
            logger.info(f"Mosaic already exists for Target: {target}, Night: {night}. Skipping.")
            continue

        # create target directory
        target_dir = work_dir / target
        target_dir.mkdir(parents=True, exist_ok=True)

        # create night directory within target
        night_dir = target_dir / str(night)
        night_dir.mkdir(parents=True, exist_ok=True)

        # collect all exposure file paths for the grouped epochs
        exposure_paths = []
        for epoch_id in epoch_ids:
            cursor.execute("SELECT file_path FROM exposures WHERE epoch_id = ?", (epoch_id,))
            exposures = cursor.fetchall()
            for exposure in exposures:
                exposure_path = Path(exposure[0])
                if exposure_path.exists():
                    exposure_paths.append(exposure_path)
                else:
                    logger.warning(f"Exposure file does not exist: {exposure_path}")

        if not exposure_paths:
            logger.warning(f"No exposure files found for Target: {target}, Night: {night}. Skipping.")
            continue

        # create soft links
        for exposure_path in exposure_paths:
            destination = night_dir / exposure_path.name
            try:
                if not destination.exists():
                    os.symlink(exposure_path.resolve(), destination)
                    logger.debug(f"Created symlink: {destination} -> {exposure_path.resolve()}")
                else:
                    logger.debug(f"Symlink already exists: {destination}")
            except OSError as e:
                logger.error(f"Failed to create symlink for {exposure_path}: {e}")

    db.close()
    logger.info("Grouping and linking process completed.")


if __name__ == "__main__":
    main()

