import os
import sys
import logging
from pathlib import Path
import numpy as np
from astropy.stats import sigma_clipped_stats
from astropy.io import fits

from logger import setup_logger
from utils import load_config
from database import Database
from swarp_caller import run_swarp


def create_noisemap(data_adu, gain, mask=None):
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
    _, _, rms_adu = sigma_clipped_stats(data_adu)
    rms_electron = rms_adu * gain
    data_electron = data_adu * gain
    noisemap_electron = np.sqrt(rms_electron**2 + np.abs(data_electron))
    if mask is not None:
        noisemap_electron[mask] = 1e8

    noisemap_adu = noisemap_electron / gain
    return noisemap_adu


def make_mosaic(target_name, night_date):
    config_path = os.environ.get('OMEGACAMER_CONFIG')
    if not config_path:
        print("Environment variable 'OMEGACAMER_CONFIG' not set.")
        sys.exit(1)

    config = load_config(config_path)

    work_dir = Path(config.get('mosaic_working_directory'))
    work_dir.mkdir(parents=True, exist_ok=True)

    mosaic_dir_path = work_dir / target_name / night_date

    log_level_str = config.get('logging', {}).get('level', 'INFO').upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    log_file = work_dir / config.get('logging', {}).get('file', 'omegacam_mosaic.log')
    logger = setup_logger(log_level, log_file)

    logger.info("Starting grouping and linking process.")

    db_name = config.get('database').get('name')
    db_path = work_dir / db_name
    db = Database(db_path)
    logger.info(f"Connected to database at {db_path}.")

    # 1. gather exposures
    exposure_paths = db.get_exposures_for_mosaic(target_name=target_name, night_date=night_date)
    for exposure_path in exposure_paths:
        # 2. create a soft link of each exposure at the directory of the mosaic -- will be used by swarp.
        soft_link = mosaic_dir_path / exposure_path.name
        if not soft_link.exists():
            os.symlink(exposure_path.resolve(), soft_link)
        # 3. make a noisemap for each exposure.
        data_adu = fits.getdata(exposure_path)
        header = fits.getheader(exposure_path)
        gain = header['GAIN']
        noisemap_adu = create_noisemap(data_adu=data_adu, gain=gain)
        weight_path = mosaic_dir_path / f"{exposure_path.stem}.weight.fits"
        fits.writeto(filename=weight_path, data=1. / noisemap_adu**2, header=header)
    # 4. ...call swarp.
    output_mosaic_file = mosaic_dir_path / f"mosaic_{target_name}_{night_date}.fits"
    weight_output_mosaic_file = mosaic_dir_path / f"mosaic_{target_name}_{night_date}.weight.fits"
    if not output_mosaic_file.exists():
        run_swarp(
            file_pattern="*FCS.fits",
            work_dir=work_dir,
            output_filename=output_mosaic_file.name,
            weight_output_filename=weight_output_mosaic_file.name,
            redo=False,
            config_file_name=f"{output_mosaic_file.stem}_config.swarp",
            subtract_back='Y'
        )

