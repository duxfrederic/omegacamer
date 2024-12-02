import yaml
import os
from pathlib import Path


class Config:
    def __init__(self, config_path=None):
        config_path = config_path or os.environ.get('OMEGACAMER_CONFIG')
        if not config_path:
            raise ValueError("Config file path not provided and OMEGACAMER_CONFIG is not set.")
        self.config_path = Path(config_path)
        if not self.config_path.is_file():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        with open(self.config_path, 'r') as f:
            self.config = yaml.safe_load(f)

    def get(self, key, default=None):
        return self.config.get(key, default)
