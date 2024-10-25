# omegacamer
download and reduce omegacam data


for now a collection of scripts, let's see:

### reduction_status.py

This script queries the ESO archive for OMEGACAM observations, compares them with the reduced data on the server, and generates an HTML report indicating the reduction status of each observation.

- ** ESO Archive Query**: Retrieves recent OMEGACAM observation records.
- **Reduction Status Comparison**: Checks which observations have been reduced or are pending based on the presence of corresponding FITS files.
- **HTML report**: Creates an HTML report summarizing the status.
- **Configuration via YAML**: configure paths, credentials, and lenses via `config.yaml`.
- **Scheduled execution**: to be set up to run automatically using `crontab`.

## Prerequisites

- Python 3.6 or higher
- Required Python packages:
  - `pandas`
  - `requests`
  - `PyYAML`
  - `astroquery`
  - `jinja2`

