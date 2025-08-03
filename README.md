# `omegacamer`

Download (from the ESO archive) and process VST/OmegaCAM images.

Workflow:
``` 
export OMEGACAMER_CONFIG="/path/to/config.yaml"  # see template at /omegacamer/config.yaml
# download
python -m omegacamer.scripts.download --start 2025-08-02 --end 2025-08-03

# pre red of not-reduced files:
python -m omegacamer.scripts.prered

# mosaic: inventory of reduced files (kept separate from the prered for now)
python omegacamer/mosaic/inventory.py
# mosaic: produce them (requires swarp and scamp installed)
python omegacamer/mosaic/make_mosaic.py
```


Warning:
to use this, you need to patch the download function of astroquery, see
https://github.com/astropy/astroquery/issues/3380


## Requirements

- Python 3.10 or higher
- Python packages: see `pyproject.toml`

