from pathlib import Path
from astropy.io import fits
from astropy.stats import sigma_clipped_stats
import numpy as np
from database import Database
from logger import setup_logger
import logging
import os
import sys

from utils import determine_night, load_config


def create_noise_map(data_adu, rms_adu, gain, mask=None):
    """
    Creates a noise map in ADU.

    Parameters:
    - data_electron: Sky-subtracted data in electrons.
    - rms_electron: RMS of the background in electrons.
    - gain: Gain value.
    - mask: Boolean array where True indicates regions to boost noise.
    - boost_factor: Factor by which to boost the noise.

    Returns:
    - noise_map_adu: Noise map in ADU.
    """
    rms_electron = rms_adu * gain
    data_electron = data_adu * gain
    noise_map_electron = np.sqrt(rms_electron**2 + np.abs(data_electron))

    if mask is not None:
        noise_map_electron[mask] = 1e8

    noise_map_adu = noise_map_electron / gain
    return noise_map_adu


def main():
    config_path = os.environ.get('OMEGACAMER_CONFIG')
    if not config_path:
        print("Environment variable 'OMEGACAMER_CONFIG' not set.")
        sys.exit(1)

    config = load_config(config_path)

    work_dir = Path(config.get('mosaic_working_directory'))
    work_dir.mkdir(parents=True, exist_ok=True)

    log_level_str = config.get('logging', {}).get('level', 'INFO').upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    log_file = work_dir / config.get('logging', {}).get('file', 'omegacam_mosaic.log')
    logger = setup_logger(log_level, log_file)

    logger.info("Starting sky subtraction and noise map computation.")

    db_name = config.get('database').get('name')
    db_path = work_dir / db_name
    db = Database(db_path)
    logger.info(f"Connected to database at {db_path}.")

    # CCD masks: generated once with ~lenses/prered_pipeline/VST/make_masks.py, masking bad columns
    ccd_masks_dir = Path(config.get('ccd_masks_directory'))
    if not ccd_masks_dir.exists() or not ccd_masks_dir.is_dir():
        logger.error(f"CCD masks directory does not exist or is not a directory: {ccd_masks_dir}")
        sys.exit(1)

    ccd_masks = {}
    for mask_file in ccd_masks_dir.glob('*.fits'):
        ccd_id_str = mask_file.stem  # assuming filename is like '10.fits'
        try:
            ccd_id = int(ccd_id_str)
            with fits.open(mask_file) as hdul:
                mask_data = hdul[0].data.astype(bool)
            ccd_masks[ccd_id] = mask_data
            logger.debug(f"Loaded mask for CCD {ccd_id} from {mask_file}.")
        except ValueError:
            logger.warning(f"Mask file {mask_file} does not have a valid CCD ID in its filename.")
        except Exception as e:
            logger.error(f"Failed to load mask file {mask_file}: {e}")

    if not ccd_masks:
        logger.warning("No CCD masks loaded. Noise maps will not be adjusted based on masks.")

    # get target-night groups without mosaics
    epochs = db.get_all_epochs()
    logger.info(f"Fetched {len(epochs)} epochs from the database.")

    # group epochs by target and night
    from collections import defaultdict
    grouped = defaultdict(list)  # key: (target, night), value: list of epoch_ids

    for epoch in epochs:
        epoch_id, target, timestamp = epoch
        night = determine_night(timestamp)
        key = (target, night)
        grouped[key].append(epoch_id)

    logger.info(f"Grouped into {len(grouped)} target-night combinations.")

    for (target, night), epoch_ids in grouped.items():
        logger.info(f"Processing Target: {target}, Night: {night}")

        # check if mosaic already exists
        existing_mosaic = db.get_mosaic(target, night)
        if existing_mosaic:
            logger.info(f"Mosaic already exists for Target: {target}, Night: {night}. Skipping.")
            continue

        target_dir = work_dir / target
        night_dir = target_dir / str(night)
        night_dir.mkdir(parents=True, exist_ok=True)

        # retrieve all exposures in the group
        exposures = []
        for epoch_id in epoch_ids:
            exp = db.get_exposures_by_epoch_id(epoch_id)
            exposures.extend(exp)

        if not exposures:
            logger.warning(f"No exposures found for Target: {target}, Night: {night}. Skipping.")
            continue

        for exposure in exposures:
            exposure_id, file_path, ccd_id = exposure
            fits_file = Path(file_path)

            if not fits_file.exists():
                logger.error(f"FITS file does not exist: {fits_file}. Skipping exposure ID {exposure_id}.")
                continue

            noise_map_path = night_dir / f"{fits_file.stem}_sky_sub.weight.fits"

            if noise_map_path.exists():
                logger.info(f"Noisemap already exists for exposure ID {exposure_id}.")

            noise_map_path = fits_file.with_name(f"{fits_file.stem}.weight.fits")

            if noise_map_path.exists():
                logger.info(f"Noisemap already exist for exposure ID {exposure_id}")
                continue

            try:
                logger.info(f"Processing exposure ID {exposure_id}: {fits_file}")

                with fits.open(fits_file) as hdul:
                    data = hdul[0].data.astype(float)
                    header = hdul[0].header

                mean, median, std = sigma_clipped_stats(data, sigma=3.0, maxiters=5)

                # noisemap in ADU
                mask = ccd_masks.get(ccd_id, None)
                noise_map_adu = create_noise_map(data, std, header['GAIN'], mask=mask)

                hdu_noise_map = fits.PrimaryHDU(data=noise_map_adu, header=header)
                hdu_noise_map.writeto(noise_map_path, overwrite=True)
                logger.debug(f"Saved noise map file: {noise_map_path}")

            except Exception as e:
                logger.error(f"Failed to process exposure ID {exposure_id}: {e}")
            break
            # for now just doing one.
            # insert mosaic creation here.

    db.close()


if __name__ == "__main__":
    main()

