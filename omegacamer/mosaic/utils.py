from datetime import datetime, timedelta
import pytz
import yaml


def load_config(config_path):
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def determine_night(timestamp_utc, timezone='America/Santiago'):
    """
    Determines the night date based on Paranal (Chile) local time.
    Night is defined as from 8 PM to 7 AM local time.

    Parameters:
    - timestamp_utc (str): Timestamp in ISO format (e.g., '2024-11-08T06:33:23.138').
    - timezone (str): Timezone string (default: 'America/Santiago').

    Returns:
    - night_date (str): Date string in 'YYYY-MM-DD' format representing the night.
    """
    tz = pytz.timezone(timezone)
    dt_utc = datetime.strptime(timestamp_utc, "%Y-%m-%dT%H:%M:%S")
    dt_local = pytz.utc.localize(dt_utc).astimezone(tz)

    if dt_local.hour >= 20:
        night_date = dt_local.date()
    else:
        night_date = (dt_local - timedelta(days=1)).date()

    return night_date.isoformat()

