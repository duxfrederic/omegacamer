import subprocess
import re
from pathlib import Path
from astropy.wcs import WCS
from astropy.io import fits
from shutil import copy2

from omegacamer.mosaic.utils import copy_static_configs
from omegacamer.mosaic.logger import setup_logger
from omegacamer.mosaic.config import Config
from omegacamer.mosaic.exceptions import HeaderNotCelestialError, NoHeaderFileProducedError


class ScampRunner:
    def __init__(self, logger=None):
        self.config = Config()
        self.scamp_bin = self.config.get('scamp_bin', 'scamp')
        self.sex_bin = self.config.get('sex_bin', 'sex')
        self.tmp_dir = Path(self.config.get('tmp_dir', '/tmp'))
        if not logger:
            self.logger = setup_logger()
        else:
            self.logger = logger
        # make sure the save dir for sources exists
        self.sources_save_dir = Path(self.config.get('sources_save_dir'))
        self.sources_save_dir.mkdir(exist_ok=True, parents=True)
        # same for headers
        self.headers_save_dir = Path(self.config.get('headers_save_dir'))
        self.headers_save_dir.mkdir(exist_ok=True, parents=True)

    def run_scamp(self, file_path: Path):
        temp_dir = self.tmp_dir / f"scamp_{file_path.stem}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Processing {file_path} at work dir {temp_dir}")

        sex_config_files = ["default.conv", "default.nnw", "default.param", "default.sex"]
        scamp_config_files = ["default.scamp"]

        temp_soft_link = temp_dir / file_path.name
        if temp_soft_link.exists():
            temp_soft_link.unlink()
        temp_soft_link.symlink_to(file_path)

        # this will be the final output
        out_header = temp_dir / f"{file_path.stem}.head"
        # which we will copy to the save directory
        out_header_save = self.headers_save_dir / out_header.name
        # we'll also save the sources!
        catalog_path = temp_dir / f"{file_path.stem}.cat"
        catalog_path_save = self.sources_save_dir / catalog_path.name
        skip_copy = False
        try:
            copy_static_configs(temp_dir, sex_config_files + scamp_config_files)
            if out_header.exists():
                self.logger.info(f'Using existing header at {out_header}')
            elif out_header_save.exists():
                self.logger.info(f'Using existing header at {out_header_save}')
                copy2(out_header_save, out_header)
                skip_copy = True

            else:
                self.logger.info(f'Running sextractor on {temp_soft_link}')

                subprocess.run(
                    [self.sex_bin, str(temp_soft_link), "-c", str(temp_dir / "default.sex"),
                     "-CATALOG_NAME", str(catalog_path)],
                    check=True,
                    cwd=str(temp_dir)
                )
                # ok; now if very few sources, we just skip as it makes scamp hang indefinitely.
                # read the catalogue file
                with open(catalog_path, 'r') as f:
                    cat_contents = f.read()
                rematch = re.search(r"SEXNFIN\s*=\s*(\d+)", cat_contents)
                if rematch:
                    try:
                        sexnfin_value = int(rematch.group(1))
                        if sexnfin_value < 3:
                            self.logger.warning(f"Too few detected sources in {catalog_path}")
                            raise NoHeaderFileProducedError
                    except ValueError as err:
                        self.logger.warning(f"Could not parse # of sources in {catalog_path}, corrupt? Exception: {err}")
                        raise NoHeaderFileProducedError
                else:
                    self.logger.warning(f"No indication of # of sources in {catalog_path}, corrupt?")
                    raise NoHeaderFileProducedError

                # if the above went through, we are good to go
                self.logger.info(f'Running scamp on {temp_soft_link}; catalogue {catalog_path}')
                subprocess.run(
                    [self.scamp_bin, str(catalog_path), "-c", str(temp_dir / "default.scamp")],
                    check=True,
                    cwd=str(temp_dir)
                )

            if not out_header.exists():
                success = False
                raise NoHeaderFileProducedError

            with open(out_header, 'r') as ff:
                header_contents = ff.read()
            # replace utf-8 character "é" (from 'université') by ascii e
            header_contents = header_contents.replace('é', 'e')
            header = fits.Header.fromstring(header_contents, sep='\n')

            # check we have a celestial looking wcs
            wcs = WCS(header)
            if not wcs.is_celestial:
                success = False
                raise HeaderNotCelestialError

            # update original fits file
            original_header = fits.getheader(file_path)
            updated_header = original_header.copy()
            # delete existing WCS cards.
            wcs_keywords = [
                "CTYPE1", "CTYPE2", "CRVAL1", "CRVAL2", "CRPIX1", "CRPIX2",
                "CD1_1", "CD1_2", "CD2_1", "CD2_2", "CDELT1", "CDELT2",
                "CROTA1", "CROTA2", "PC1_1", "PC1_2", "PC2_1", "PC2_2",
                "LONPOLE", "LATPOLE", "EQUINOX", "RADESYS"
            ]

            for keyword in wcs_keywords:
                if keyword in updated_header:
                    del updated_header[keyword]

            # fold in the new ones
            updated_header.update(wcs.to_header())
            updated_header['SCMP_SLV'] = "1"

            # aand update original fits file.
            with fits.open(file_path, mode='update') as ff:
                ff[0].header = updated_header
            self.logger.info(f"Updated WCS of {file_path}")
            success = True

            # at this point we're satisfied and copy the products
            if not skip_copy:
                copy2(out_header, out_header_save)
                copy2(catalog_path, catalog_path_save)

        except subprocess.CalledProcessError as E:
            self.logger.error(f'Error running sex or scamp: {temp_soft_link}; error {E}.')
            success = False
        except NoHeaderFileProducedError:
            self.logger.error(f'Error: header file was not created for file {temp_soft_link}')
            success = False
        except HeaderNotCelestialError:
            self.logger.error(f'Error: header file does not contain WCS information? {out_header}')
            success = False
        finally:
            if not self.config.get('keep_tmp', False):
                for item in temp_dir.iterdir():
                    item.unlink()
                temp_dir.rmdir()

        return success, catalog_path_save, out_header_save
