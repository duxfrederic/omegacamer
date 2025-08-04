import argparse
import os
from pathlib import Path
from collections import defaultdict
from typing import Dict, List
from astropy.time import Time
from astropy.io.fits.verify import VerifyWarning
import warnings
warnings.simplefilter("ignore", VerifyWarning)

from omegacamer.mosaic.utils import load_config
from omegacamer.prered.database_manager import DatabaseManager
from omegacamer.prered.pipeline import process_object


def main(start_date: str = None, end_date: str = None) -> None:
    cfg = load_config(os.environ["OMEGACAMER_CONFIG"])
    workdir = Path(cfg["working_directory"]).expanduser().resolve()
    os.chdir(workdir)

    db = DatabaseManager(os.environ["OMEGACAMER_CONFIG"])

    mjd_filter = ""
    if start_date:
        mjd_start = Time(start_date).mjd
        mjd_filter += f" AND mjd_obs >= {mjd_start}"
    if end_date:
        mjd_end = Time(end_date).mjd
        mjd_filter += f" AND mjd_obs <= {mjd_end}"

    # raw science frames missing reduction
    unreduced = db.conn.execute(
        f"""
        SELECT * FROM raw_science_files AS s
         WHERE NOT EXISTS (
              SELECT 1 FROM reduced_science_files AS r
               WHERE r.raw_dp_id = s.dp_id
         )
         {mjd_filter};
        """
    ).fetchall()

    if not unreduced:
        print("Nothing to do (all science frames already reduced).")
        return

    # cluster by object
    per_object: Dict[str, List[dict]] = defaultdict(list)
    for row in unreduced:
        per_object[row["object"] or "UNKNOWN"].append(row)

    for obj in sorted(per_object):
        process_object(db, per_object[obj], cfg)


def cli_main():
    parser = argparse.ArgumentParser(description="Reduce OmegaCAM science frames.")
    parser.add_argument("--start", help="Start date in YYYY-MM-DD format (optional)")
    parser.add_argument("--end", help="End date in YYYY-MM-DD format (optional)")

    args = parser.parse_args()
    main(start_date=args.start, end_date=args.end)


if __name__ == "__main__":
    cli_main()