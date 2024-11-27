import os
import sys
import logging
from pathlib import Path
import numpy as np
from astropy.io import fits
from astropy.table import Table
import re
import sep

from widefield_plate_solver import plate_solve

from logger import setup_logger
from utils import load_config
from database import Database
from swarp_caller import run_swarp


def create_noisemap(data_adu_sky_sub, gain, rms_adu, mask):
    """
    Creates a noise map in ADU.

    Parameters:
    - data_adu_sky_sub: Sky-subtracted data.
    - gain: Gain value.
    - rms_adu: std of background
    - mask: Boolean array where True indicates regions to boost noise.

    Returns:
    - noise_map_adu: Noise map in ADU.
    """
    rms_electron = rms_adu * gain
    data_electron = data_adu_sky_sub * gain
    noisemap_electron = np.sqrt(rms_electron**2 + np.abs(data_electron))
    if mask is not None:
        noisemap_electron[mask] = 1e8

    noisemap_adu = noisemap_electron / gain
    return noisemap_adu


def extract_ccd_number_from_filename(filename):
    match = re.search(r"_(\d+)OFCS", filename)
    return int(match.group(1)) if match else None


def make_mosaic(target_name, night_date):
    config_path = os.environ.get('OMEGACAMER_CONFIG')
    if not config_path:
        print("Environment variable 'OMEGACAMER_CONFIG' not set.")
        sys.exit(1)

    config = load_config(config_path)

    work_dir = Path(config.get('mosaic_working_directory'))

    mosaic_dir_path = work_dir / target_name / night_date
    mosaic_dir_path.mkdir(parents=True, exist_ok=True)

    log_level_str = config.get('logging', {}).get('level', 'INFO').upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    log_file = work_dir / config.get('logging', {}).get('file', 'omegacam_mosaic.log')
    logger = setup_logger(log_level, log_file)
    logger.info(f'Making mosaic for target {target_name}, night of the {night_date}')

    db_name = config.get('database').get('name')
    db_path = work_dir / db_name
    db = Database(db_path)
    logger.info(f"Connected to database at {db_path}.")
    # 1. gather exposures
    exposure_paths = db.get_exposures_for_mosaic(target_name=target_name, night_date=night_date)
    if len(exposure_paths) % 32 != 0:
        logger.warning(f"Skipping mosaic for target {target_name} on the night of the {night_date}"
                       f": it seems we do not have CCDs, total number ({len(exposure_paths)} not a multiple of 32."
                       "Skipping.")
        # do nothing
        return

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

    temporary_files = []

    logger.info(f"Found {len(exposure_paths)} exposures for target {target_name}, in the night of the {night_date}.")
    for ii, exposure_path in enumerate(exposure_paths):
        header = fits.getheader(exposure_path)
        exposure_path = Path(exposure_path)
        ccd_number = extract_ccd_number_from_filename(exposure_path.name)
        mask = ccd_masks[ccd_number]
        # 2. background subtract and extract sources
        skysub_path = mosaic_dir_path / exposure_path.name
        data = fits.getdata(exposure_path).astype(np.float32)
        bg = sep.Background(data, mask=(data > 2e4), bh=300, bw=300)
        rms_adu = bg.globalrms
        data_skysub = data - bg.back()
        if not skysub_path.exists():
            fits.writeto(skysub_path, header=header, data=data_skysub)
        temporary_files.append(skysub_path)
        logger.info(f"Sky-subtracted exposure {ii+1}/{len(exposure_paths)}")

        # 3. make a noisemap for each exposure.
        weight_path = mosaic_dir_path / f"{exposure_path.stem}.weight.fits"
        if not weight_path.exists():
            gain = header['GAIN']
            noisemap_adu = create_noisemap(data_adu_sky_sub=data_skysub, gain=gain, mask=mask, rms_adu=rms_adu)
            fits.writeto(filename=weight_path, data=1. / noisemap_adu**2, header=header)
            logger.info(f"Wrote weights file: {weight_path}")
        else:
            logger.info(f"Weights file already exists: {weight_path}")
            noisemap_adu = fits.getdata(weight_path).astype(np.float32)
        temporary_files.append(weight_path)

        # 3. Plate solve the exposure.
        if not config.get('already_plate_solved'):
            # no adverse effect in skipping this in principle.
            sep.set_extract_pixstack(3_000_000)
            sources = Table(
                sep.extract(data_skysub, thresh=7.5, minarea=25,
                            mask=ccd_masks[ccd_number] + (data_skysub > 5e4),
                            err=noisemap_adu)
            )
            sources['xcentroid'] = sources['x']
            sources['ycentroid'] = sources['y']  # for plate solve below, needs xcentroid and ycentroid
            logger.info(f"Extracted {len(sources)} sources from exposure {ii+1}/{len(exposure_paths)}"
                        f" ({exposure_path.name})")

            wcs = plate_solve(fits_file_path=skysub_path, sources=sources,
                              use_api=False, use_n_brightest_only=100, do_debug_plot=False,
                              use_existing_wcs_as_guess=True, logger=logger)
            wcs['PL-SLVED'] = 'done'
            # also update the original fits file, this way it is plate solved.
            with fits.open(exposure_path, mode='update') as hdul:
                hdul[0].header.update(wcs)
                hdul.flush()

    # 5. ...call swarp.
    output_mosaic_file = mosaic_dir_path / f"mosaic_{target_name}_{night_date}.fits"
    weight_output_mosaic_file = mosaic_dir_path / f"mosaic_{target_name}_{night_date}.weight.fits"
    if not output_mosaic_file.exists():
        logger.info(f"Calling swarp at {mosaic_dir_path}.")
        run_swarp(
            file_pattern="*FCS.fits",
            weight_type="MAP_WEIGHT",
            work_dir=mosaic_dir_path,
            output_filename=output_mosaic_file.name,
            weight_output_filename=weight_output_mosaic_file.name,
            redo=False,
            config_file_name=f"{output_mosaic_file.stem}_config.swarp",
            subtract_back='Y'
        )
        if output_mosaic_file.exists():
            logger.info(f"Swarp produced a file: {output_mosaic_file}. Adding to DB.")
            db.add_mosaic(target_name=target_name, night_date=night_date, mosaic_file_path=str(output_mosaic_file))
        else:
            logger.error(f"Swarp failed to produce the mosaic file! Directory: {mosaic_dir_path}")
    else:
        logger.warning(f"Mosaic file already exists, inconsistency with database?")

    for path in temporary_files:
        path.unlink()


if __name__ == "__main__":
    config_path = os.environ.get('OMEGACAMER_CONFIG')
    if not config_path:
        print("Environment variable 'OMEGACAMER_CONFIG' not set.")
        sys.exit(1)

    config = load_config(config_path)

    work_dir = Path(config.get('mosaic_working_directory'))
    db_name = config.get('database').get('name')
    db_path = work_dir / db_name
    db = Database(db_path)
    nights_targets = db.get_missing_mosaics()
    db.close()
    print(f"{len(nights_targets)} mosaics to make.")
    for (night, target) in nights_targets:
        make_mosaic(target, night)
