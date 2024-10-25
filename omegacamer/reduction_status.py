#!/usr/bin/env python

from pathlib import Path
import pandas as pd
import yaml
from jinja2 import Environment, FileSystemLoader
import logging
import sys


from omegacamdownloader import get_omegacam_observation_records


# setup logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def load_config(config_path: Path) -> dict:
    """Load configuration from a YAML file."""
    if not config_path.exists():
        logger.error(f"Configuration file {config_path} does not exist.")
        sys.exit(1)
    with config_path.open('r') as file:
        config = yaml.safe_load(file)
    return config

def query_eso_archive(config: dict) -> pd.DataFrame:
    """
    Query the ESO archive and return observation records as a DataFrame.
    """
    working_directory = config['working_directory']
    credentials = config['credentials']
    program_id = credentials['program_id']
    user = credentials['user']
    
    # Define date range for the query
    # For example, last 30 days
    from datetime import datetime, timedelta
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    logger.info("Querying ESO archive...")
    records_csv_path = get_omegacam_observation_records(
        download_directory=working_directory,
        start_date_str=start_date_str,
        end_date_str=end_date_str,
        program_id=program_id
    )
    
    logger.info(f"Reading observation records from {records_csv_path}")
    obs_df = pd.read_csv(records_csv_path, comment='#')
    return obs_df

def load_reduced_data(config: dict) -> dict:
    """
    Load reduced data directories for each lens.
    Returns a dictionary with lens names as keys and Path objects as values.
    """
    reduced_data_dir = Path(config['reduced_data_dir'])
    lenses = config['lenses']
    reduced_dirs = {}
    for lens in lenses:
        lens_path = reduced_data_dir / lens
        if not lens_path.exists():
            logger.warning(f"Reduced data directory for lens '{lens}' does not exist at {lens_path}.")
        reduced_dirs[lens] = lens_path
    return reduced_dirs

def check_reduction_status(obs_df: pd.DataFrame, reduced_dirs: dict, lenses: list) -> dict:
    """
    Check which observations have been reduced or are pending.
    Returns a dictionary with lens names as keys and lists of reduced and pending dp_ids.
    """
    status = {}
    for lens in lenses:
        logger.info(f"Processing lens: {lens}")
        lens_obs = obs_df[obs_df['OBJECT'] == lens]
        dp_ids = lens_obs['Dataset ID'].unique()
        reduced = []
        pending = []
        lens_dir = reduced_dirs.get(lens, None)
        if lens_dir and lens_dir.exists():
            fits_files = list(lens_dir.glob('*.fits'))
            fits_filenames = [f.name for f in fits_files]
            for dp_id in dp_ids:
                if any(dp_id in fname for fname in fits_filenames):
                    reduced.append(dp_id)
                else:
                    pending.append(dp_id)
        else:
            logger.warning(f"Lens directory for '{lens}' does not exist. All observations are pending.")
            pending = list(dp_ids)
        status[lens] = {
            'reduced': reduced,
            'pending': pending
        }
    return status

def generate_html_report(status: dict, report_path: Path):
    """
    Generate an HTML report from the status dictionary.
    """
    logger.info(f"Generating HTML report at {report_path}")
    
    # Set up Jinja2 environment
    env = Environment(loader=FileSystemLoader(searchpath=Path(__file__).parent))
    template = env.from_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Reduction Status Report</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            h1 { text-align: center; }
            table { width: 100%; border-collapse: collapse; margin-bottom: 40px; }
            th, td { border: 1px solid #dddddd; text-align: left; padding: 8px; }
            th { background-color: #f2f2f2; }
            .reduced { color: green; }
            .pending { color: orange; }
        </style>
    </head>
    <body>
        <h1>Reduction Status Report</h1>
        {% for lens, data in status.items() %}
            <h2>Lens: {{ lens }}</h2>
            <table>
                <tr>
                    <th>Category</th>
                    <th>Dataset IDs</th>
                </tr>
                <tr>
                    <td class="reduced">Reduced</td>
                    <td>
                        {% if data.reduced %}
                            <ul>
                                {% for dp_id in data.reduced %}
                                    <li>{{ dp_id }}</li>
                                {% endfor %}
                            </ul>
                        {% else %}
                            None
                        {% endif %}
                    </td>
                </tr>
                <tr>
                    <td class="pending">Pending</td>
                    <td>
                        {% if data.pending %}
                            <ul>
                                {% for dp_id in data.pending %}
                                    <li>{{ dp_id }}</li>
                                {% endfor %}
                            </ul>
                        {% else %}
                            None
                        {% endif %}
                    </td>
                </tr>
            </table>
        {% endfor %}
    </body>
    </html>
    """)
    
    html_content = template.render(status=status)
    
    # Ensure the parent directory exists
    report_path.parent.mkdir(parents=True, exist_ok=True)
    
    with report_path.open('w') as f:
        f.write(html_content)
    logger.info("HTML report generated successfully.")

def main():
    # Define paths
    script_dir = Path(__file__).parent
    config_path = script_dir / 'config.yaml'
    
    # Load configuration
    config = load_config(config_path)
    
    # Query ESO archive
    obs_df = query_eso_archive(config)
    
    # Load reduced data directories
    reduced_dirs = load_reduced_data(config)
    
    # Check reduction status
    status = check_reduction_status(obs_df, reduced_dirs, config['lenses'])
    
    # Generate HTML report
    report_path = Path(config['report_path'])
    generate_html_report(status, report_path)
    
    logger.info("Reduction status check completed successfully.")

if __name__ == "__main__":
    main()

