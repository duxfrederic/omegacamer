from pathlib import Path
import yaml
import os
import argparse
import sys

from omegacamer.prered.database_manager import DatabaseManager
from omegacamer.prered.omegacam_downloader import get_omegacam_observation_records, download_omegacam_observations


config_path = os.environ['OMEGACAMER_CONFIG']
with open(config_path, 'r') as file:
    config = yaml.safe_load(file)
working_directory = config['working_directory']
Path(working_directory).mkdir(exist_ok=True, parents=True)
os.chdir(working_directory)
db_manager_instance = DatabaseManager(config_path=config_path)
credentials = config['credentials']
prog_id = credentials['program_id']


def main(start_date, end_date):
    try:
        out_file = get_omegacam_observation_records(start_date, end_date, prog_id)
        download_omegacam_observations(out_file, db_manager_instance)
    except Exception as e:
        print('Error:', e, file=sys.stderr)
        raise
    finally:
        db_manager_instance.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download and register OmegaCAM observations.')
    parser.add_argument('--start', required=True, help='Start date in format YYYY-MM-DD')
    parser.add_argument('--end', required=True, help='End date in format YYYY-MM-DD')

    args = parser.parse_args()

    main(args.start.replace('-', ' '), args.end.replace('-', ' '))

