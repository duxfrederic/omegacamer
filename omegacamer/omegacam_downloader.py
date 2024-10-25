from pathlib import Path
import pandas as pd
import requests
import yaml
from astroquery.eso import EsoClass
from database_manager import DatabaseManager

# load config
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

working_directory = config['working_directory']
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


def get_omegacam_observation_records(download_directory, start_date_str, end_date_str, program_id):
    """
    Downloads the observation records CSV if it doesn't already exist.
    """
    download_dir = Path(download_directory)
    out_file = download_dir / f"records_{start_date_str.replace(' ', '-')}_{end_date_str.replace(' ', '-')}.csv"
    if out_file.exists():
        return out_file

    download_dir.mkdir(parents=True, exist_ok=True)
    url = format_url(omegacam_url_template, start_date_str, end_date_str, program_id)
    download_file(url, out_file)
    return out_file


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
        if db_manager.record_exists(dp_id):
            print(f"Dataset ID {dp_id} already downloaded. Skipping.")
            continue
        to_download.append(dp_id)

    # get associated calibrations:
    calib_files = esoclass.get_associated_files(to_download)
    downloaded_paths = esoclass.retrieve_data(calib_files, destination=working_directory, with_calib=None,
                                              unzip=True)

    for raw_science_dp_id in to_download:
        file_path = esoclass.retrieve_data(raw_science_dp_id, destination=working_directory, with_calib=None,
                                           unzip=True)
        print(f"Downloaded {raw_science_dp_id} to {file_path}")
        record = obs_records[obs_records['Dataset ID'] == raw_science_dp_id].iloc[0]
        record_dict = record.to_dict()
        record_dict['save_path'] = file_path
        added = db_manager.add_record(record_dict, working_directory)
        if added:
            print(f"Recorded download of {raw_science_dp_id} in the database.")


if __name__ == "__main__":
    db_manager = DatabaseManager(config_path='config.yaml')

    try:
        start_date = '2024 10 24'
        end_date = '2024 10 25'

        out_file = get_omegacam_observation_records(working_directory, start_date, end_date, prog_id)

        download_omegacam_observations(out_file, db_manager)
    finally:
        db_manager.close()
