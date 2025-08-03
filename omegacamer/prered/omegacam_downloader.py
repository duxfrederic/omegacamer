from pathlib import Path
import pandas as pd
import requests
import yaml
from astropy.io import fits
from astroquery.eso import EsoClass
import os

from database_manager import DatabaseManager

# load config
config_path = os.environ['OMEGACAMER_CONFIG']
with open(config_path, 'r') as file:
    config = yaml.safe_load(file)

working_directory = config['working_directory']
Path(working_directory).mkdir(exist_ok=True, parents=True)
credentials = config['credentials']
prog_id = credentials['program_id']
user = credentials['user']

omegacam_url_template = ("https://archive.eso.org/wdb/wdb/eso/eso_archive_main/query?wdbo=csv%2fdownload"
                         "&max_rows_returned={max_returned}&instrument=&target=&resolver=simbad&ra=&dec="
                         "&box=00%2010%2000&degrees_or_hours=hours&tab_target_coord=on&format=SexaHour"
                         "&wdb_input_file=&night=&stime={start_date_str}&starttime=12&etime={end_date_str}"
                         "&endtime=12&tab_prog_id=on&prog_id={program_id}&gto=&pi_coi="
                         "&obs_mode=&title=&image[]=OMEGACAM&tab_dp_cat=on"
                         "&tab_dp_type=on&dp_type=&dp_type_user=&tab_dp_tech=on"
                         "&dp_tech=&dp_tech_user=&tab_dp_id=on&dp_id="
                         "&origfile=&tab_rel_date=on&rel_date=&obs_name=&ob_id="
                         "&tab_tpl_start=on&tpl_start=&tab_tpl_id=on&tpl_id="
                         "&tab_exptime=on&exptime=&tab_filter_path=on&filter_path="
                         "&tab_wavelength_input=on&wavelength_input=&tab_fwhm_input=on"
                         "&fwhm_input=&gris_path=&grat_path=&slit_path="
                         "&tab_instrument=on&add=((ins_id%20like%20%27OMEGACAM%25%27))"
                         "&tab_tel_airm_start=on&tab_stat_instrument=on&tab_ambient=on"
                         "&tab_stat_exptime=on&tab_HDR=on&tab_mjd_obs=on"
                         "&aladin_colour=aladin_instrument&tab_stat_plot=on&order=&")


def format_url(template, start_date_str, end_date_str, program_id, max_returned=30000):
    """
    Formats the template URL with start and end dates, program ID, and max returned rows.
    """
    url = template.format(start_date_str=start_date_str, end_date_str=end_date_str,
                          max_returned=max_returned, program_id=program_id)
    url = url.replace(' ', '%20')
    return url


def download_file(url, save_path):
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(save_path, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)


def get_omegacam_observation_records(start_date_str, end_date_str, program_id):
    """
    Downloads the observation records CSV if it doesn't already exist.
    """
    download_dir = Path('obs_records')
    out_file = download_dir / f"records_{start_date_str.replace(' ', '-')}_{end_date_str.replace(' ', '-')}.csv"
    if out_file.exists():
        return out_file

    download_dir.mkdir(parents=True, exist_ok=True)
    url = format_url(omegacam_url_template, start_date_str, end_date_str, program_id)
    download_file(url, out_file)
    return out_file


def get_information_from_header(header):
    """
    Gets information. object? filter binning? etc.
    """
    obj = header['OBJECT']
    mjd_obs = float(header['MJD-OBS'])
    filter_ = header['HIERARCH ESO INS FILT1 NAME']
    binning = 'x'.join([str(header[f'HIERARCH ESO DET WIN1 BIN{d}']) for d in 'XY'])  # e.g. 1x1, or 2x2, or 2x1, etc.
    readout_mode = header['HIERARCH ESO DET READ MODE']
    exptime = float(header['exptime'])
    return {'object_': obj, 'mjd_obs': mjd_obs, 'filter_': filter_, 'binning': binning, 'readout_mode': readout_mode,
            'exptime': exptime}


def download_omegacam_observations(observation_records_csv_path, db_manager):
    """
    Processes the observation records and updates the database.
    """
    obs_records = pd.read_csv(observation_records_csv_path, comment='#')
    # Initialize ESO Class with credentials
    esoclass = EsoClass()
    esoclass.login(username=credentials['user'], store_password=True)

    to_download = []
    for _, record in obs_records.iterrows():
        dp_id = record['Dataset ID']
        if db_manager.raw_science_exists(dp_id):
            print(f"Science file with ID {dp_id} already downloaded. Skipping.")
            continue
        to_download.append(dp_id)

    if not to_download:
        # then calibrations were already downloaded anyway per order of operations below.
        # we simply do nothing.
        return

    # get associated calibrations:
    calib_dp_ids = esoclass.get_associated_files(to_download)

    # download and register each relevant calibration file
    calib_dest_path = Path('raw/calib')
    calib_dest_path.mkdir(exist_ok=True, parents=True)
    for calib_dp_id in calib_dp_ids:
        if calib_dp_id.startswith('M.'):  # combined calibration or old star catalogue, we do not need these.
            continue

        if (
                db_manager.flat_exists(calib_dp_id)
                or db_manager.bias_exists(calib_dp_id)
                or db_manager.unusedcalib_exists(calib_dp_id)
            ):
            print(f"Calibration with dataset ID {calib_dp_id} already downloaded. Skipping.")
            continue

        calib_path = esoclass.retrieve_data(calib_dp_id, destination=calib_dest_path, with_calib=None,
                                             unzip=False, continuation=False)
        # path given by esoclass insists on being absolute, we want it relative.
        calib_path = Path(calib_path).relative_to(working_directory)
        header = fits.getheader(calib_path)  # first card header has the information we need on omegacam
        header_info = get_information_from_header(header)
        if header_info['object_'] == 'FLAT,SKY' or header_info['object_'] == 'FLAT,DOME':
            flat_type = header_info['object_'].split(',')[1]
            del header_info['exptime']
            del header_info['object_']
            db_manager.register_flat(calib_id=calib_dp_id, **header_info, path=calib_path,
                                     type_=flat_type)
            print(f"Downloaded {flat_type} {calib_dp_id} to {calib_path}")
        elif header_info['object_'] == 'BIAS':
            del header_info['filter_']
            del header_info['exptime']
            del header_info['object_']
            db_manager.register_bias(calib_id=calib_dp_id, **header_info, path=calib_path)
            print(f"Downloaded BIAS {calib_dp_id} to {calib_path}")
        else:
            # not a calibration we are interested in
            db_manager.register_unused_calib(calib_id=calib_dp_id, type_=header_info['object_'])
            print(f"Calibration {calib_dp_id} has uncaught type: {header_info['object_']}.")
            continue


    # now do the same with science files.
    science_dest_path = Path('raw/science')
    science_dest_path.mkdir(exist_ok=True, parents=True)
    for raw_science_dp_id in to_download:
        if db_manager.raw_science_exists(raw_science_dp_id):
            print(f"Science file with dataset ID {raw_science_dp_id} already downloaded. Skipping.")
            continue
        file_path = esoclass.retrieve_data(raw_science_dp_id, destination=science_dest_path, with_calib=None,
                                           unzip=False, continuation=False)
        # file_path insists on being absolute, we want it relative.
        file_path = Path(file_path).relative_to(working_directory)
        header = fits.getheader(file_path)  # first card header has the information we need on omegacam
        header_info = get_information_from_header(header)
        db_manager.register_raw_science(dp_id=raw_science_dp_id, **header_info, path=file_path)
        print(f"Downloaded science file {raw_science_dp_id} (object {header_info['object_']}) to {file_path}")


if __name__ == "__main__":
    os.chdir(working_directory)
    import os
    config_path = os.environ['OMEGACAMER_CONFIG']
    db_manager_instance = DatabaseManager(config_path=config_path)

    try:
        start_date = '2025 07 27'
        end_date = '2025 07 28'

        out_file = get_omegacam_observation_records(start_date, end_date, prog_id)

        download_omegacam_observations(out_file, db_manager_instance)
    finally:
        db_manager_instance.close()
