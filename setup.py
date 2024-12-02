from setuptools import setup, find_packages

setup(
    name="omegacamer",
    version="0.0.1",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "omegacamer.mosaic.static_configs": ["*"],  # Include all files in the static_configs directory
    },
    install_requires=[
        "pyyaml",
        "astropy"
    ],
)
