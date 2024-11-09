import yaml
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from astropy.io import fits
from astropy.stats import sigma_clipped_stats
from ccdproc import CCDData
import sep
import numpy as np
from database import Database
from logger import setup_logger
import logging
import os
import sys

def load_config(config_path):
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def create_noise_map(data_electron, rms_electron, gain, mask=None):
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

    # Get CCD masks
    ccd_masks_dir = Path(config.get('ccd_masks_directory'))
    if not ccd_masks_dir.exists() or not ccd_masks_dir.is_dir():
        logger.error(f"CCD masks directory does not exist or is not a directory: {ccd_masks_dir}")
        sys.exit(1)

    # Load all CCD masks into a dictionary
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
    target_night_groups = db.get_target_night_without_mosaic()
    logger.info(f"Found {len(target_night_groups)} target-night groups without mosaics.")

    for group in target_night_groups:
        target, night = group
        logger.info(f"Processing Target: {target}, Night: {night}")

        # retrieve exposures for this group
        exposures = db.get_exposures_by_target_night(target, night)
        logger.info(f"Found {len(exposures)} exposures for Target: {target}, Night: {night}")

        if not exposures:
            logger.warning(f"No exposures found for Target: {target}, Night: {night}. Skipping.")
            continue

        target_dir = work_dir / target
        night_dir = target_dir / str(night)
        night_dir.mkdir(parents=True, exist_ok=True)

        for exposure in exposures:
            exposure_id, file_path, ccd_id = exposure
            fits_file = Path(file_path)
            gain = fits.getheader(fits_file)['GAIN']

            if not fits_file.exists():
                logger.error(f"FITS file does not exist: {fits_file}. Skipping exposure ID {exposure_id}.")
                continue

            noise_map_path = fits_file.with_name(f"{fits_file.stem}_sky_sub.weight.fits")

            if noise_map_path.exists():
                logger.info(f"Noisemap already exist for exposure ID {exposure_id}")
                continue

            try:
                logger.info(f"Processing exposure ID {exposure_id}: {fits_file}")

                with fits.open(fits_file) as hdul:
                    data = hdul[0].data.astype(float)
                    header = hdul[0].header

                data_electron = data * gain

                mean, median, std = sigma_clipped_stats(data_electron, sigma=3.0, maxiters=5)
                rms_electron = std  # RMS of the background

                # noisemap in ADU
                mask = ccd_masks.get(ccd_id, None)
                noise_map_adu = create_noise_map(sky_sub_electron, rms_electron, gain, mask=mask)

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

