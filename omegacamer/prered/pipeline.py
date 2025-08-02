from __future__ import annotations
"""pipeline.py
===============

Flow: (roughly one night == one run)

1.  Get raw science exposures that have no reduced product yet.
2.  Group them by observing night (UTC).  We use
    `night_id = int(mjd_obs + 0.5)` which corresponds to midnight boundary.
3.  For each night
      ├─ build / reuse the relevant *combined* bias & flat per
      │   (filter, binning, readout_mode)
      ├─ reduce every raw science mosaic, CCD‑by‑CCD (streaming, RAM‑safe)
      └─ write results either as a single MEF file **or** one FITS per CCD
         (toggle via `config['reduced_format']`).
4.  Register outputs in `DatabaseManager` with full provenance so reruns are
    idempotent.

Directory tree (all paths relative to working_directory):

```
raw/                       # nightly downloads live here
calib/biases/              # master_bias_20241024_FAST_1x1.fits
calib/flats/               # master_flat_r_20241024_FAST_1x1.fits
reduced/<night_id>/        # reduced science frames (MEF or CCD‑splits)
```

Notes
-----
* We assume the process is started with the current working directory set
  to `config['working_directory']` so that relative paths resolve
  correctly.  `os.chdir()` is done in `main()` for safety.
* Requires: `astropy`, `numpy`, `tqdm`.
"""

from collections import defaultdict
import os
from pathlib import Path
from astropy.time import Time
from typing import Dict, List, Tuple

from astropy.io import fits
from tqdm import tqdm

from omegacamer.prered.database_manager import DatabaseManager
from omegacamer.prered.combine_calibrations import (
    build_combined_bias,
    build_combined_flat,
)
from omegacamer.mosaic.utils import determine_night, load_config
from omegacamer.prered.utils import crop_omegacam_overscan


def night_id(mjd: float) -> str:
    """ takes the mjd of the obs, returns string YYYY-MM-DD of the evening preceding
    the night."""
    t = Time(mjd, format='mjd')
    return determine_night(t.datetime.strftime("%Y-%m-%dT%H:%M:%S"))


def reduce_science_frame(
    db: DatabaseManager,
    row: Dict,
    combined_bias_id: int,
    combined_flat_id: int,
    out_dir: Path,
    mode: str = "MEF",  # or "perccd", see config.yaml
) -> Path:
    """Calibrate a *single* raw science mosaic.

    Parameters
    ----------
    db
        The live DatabaseManager instance.
    row
        Row from `raw_science_files` (dict‑like `sqlite3.Row`).
    combined_bias_id
        FK into combined_biases table.
    combined_flat_id
        FK into combined_flats table.
    out_dir
        Directory where the reduced product(s) will be written.
    mode
        'MEF'  → write a single multi‑extension FITS file (default)
        'perccd' → write individual CCD FITS files, suffixed by `_ccdNN.fits`

    Returns
    -------
    Path
        Relative path (stringified) to the output *file* **or** directory
        (when mode == 'perccd').  This is what gets stored in the DB.
    """

    raw_path = Path(row["file_path"])  # relative
    # discover mosaic structure (should be always the same)
    with fits.open(raw_path, memmap=True) as hdul:
        n_ccd = len(hdul) - 1
        phdr = hdul[0].header.copy()

    # bias and flat paths
    bias_path = db.conn.execute(
        "SELECT file_path FROM combined_biases WHERE id=?;",
        (combined_bias_id,),
    ).fetchone()[0]
    flat_path = db.conn.execute(
        "SELECT file_path FROM combined_flats WHERE id=?;",
        (combined_flat_id,),
    ).fetchone()[0]
    if not (bias_path and flat_path):
        raise RuntimeError(f'ERROR: no bias or flat for science file {raw_path}')

    out_dir.mkdir(parents=True, exist_ok=True)

    if mode.upper() == "MEF":
        out_name = raw_path.with_suffix("").name + "_red.fits"
        out_path = out_dir / out_name
        hdul_out = fits.HDUList([fits.PrimaryHDU(header=phdr)])
        for _ in range(n_ccd):
            hdul_out.append(fits.ImageHDU(data=None, header=None))
        hdul_out.writeto(out_path, overwrite=True, output_verify="ignore")

    for ccd in range(1, n_ccd + 1):
        with fits.open(raw_path, memmap=True) as h_raw:
            data = crop_omegacam_overscan(h_raw[ccd].data.astype("float32"), ccd_number=ccd)
            hdr = h_raw[ccd].header.copy()
        with fits.open(bias_path, memmap=True) as h_bias:
            data -= h_bias[ccd].data.astype("float32")
        with fits.open(flat_path, memmap=True) as h_flat:
            data /= h_flat[ccd].data.astype("float32")

        # write out
        if mode.upper() == "MEF":
            with fits.open(out_path, mode="update", memmap=False, output_verify='ignore') as h_out:
                h_out[ccd].data = data.astype("uint16")
                h_out[ccd].header.extend(hdr, update=True)
        else:  # per‑CCD
            out_path = out_dir / f"{raw_path.name.split('.fits')[0]}_{ccd}OFCS.fits" # 'OFCS' to match output of Martin's pipeline
            header = phdr.copy()
            header.extend(hdr, update=True)
            # compatibility with Martin's pipeline
            header['GAIN'] = header['HIERARCH ESO DET OUT1 GAIN']
            header['OBJECT'] = header['OBJECT'].replace(' ', '_')
            fits.writeto(out_path, data.astype("uint16"), header=header, overwrite=True,
                         output_verify="ignore")

    # MEX file if MEX reduction, last reduced CCD if per-ccd file writing.
    return out_path


def process_object(db: DatabaseManager, rows: List[dict], cfg: dict) -> None:
    """Calibrate the unreduced science exposures belonging to one object."""

    obj_name = rows[0]["object"] or "UNKNOWN"
    print(f"\nObject '{obj_name}': {len(rows)} raw frames to reduce")

    # lazy cache: (night, filter, bin, ro) → (bias_id, flat_id)
    calibs: Dict[Tuple[str, str, str, str], Tuple[int, int]] = {}

    out_dir = Path("reduced") / obj_name.replace(' ', '_')

    for row in tqdm(rows, desc=f"{obj_name}"):
        night = night_id(row["mjd_obs"])  # 'YYYY‑MM‑DD'
        key = (night, row["filter"], row["binning"], row["readout_mode"])

        # build / fetch calibs
        if key not in calibs:
            night_str, flt, binning, ro = key
            bias_ids = [b["calib_id"] for b in db.find_biases(
                binning=binning,
                readout_mode=ro,
                mjd_window=0.5,
                mjd_center=row["mjd_obs"],  # within same night
            )]
            if not bias_ids:
                raise RuntimeError(
                    f"No biases for {binning}/{ro} on night {night_str}")
            bias_rel = Path("calib/biases") / f"master_bias_{night_str}_{ro}_{binning}.fits"
            bias_rel.parent.mkdir(exist_ok=True, parents=True)
            bias_id = build_combined_bias(
                db,
                bias_ids=bias_ids,
                binning=binning,
                readout_mode=ro,
                output_rel=str(bias_rel),
            )

            flat_ids = [f["calib_id"] for f in db.find_flats(
                filter_=flt,
                type_='SKY',
                binning=binning,
                readout_mode=ro,
                mjd_window=0.5,
                mjd_center=row["mjd_obs"],
            )]
            if len(flat_ids) < 4:
                # fall back to dome flats
                flat_ids.extend([f["calib_id"] for f in db.find_flats(
                    filter_=flt,
                    type_='DOME',
                    binning=binning,
                    readout_mode=ro,
                    mjd_window=0.5,
                    mjd_center=row["mjd_obs"],
                )])
                if len(flat_ids) < 4:
                    raise RuntimeError(
                        f"No flats for {flt}/{binning}/{ro} on night {night_str}")
            flat_rel = Path("calib/flats") / f"master_flat_{flt}_{night_str}_{ro}_{binning}.fits"
            flat_rel.parent.mkdir(exist_ok=True, parents=True)
            flat_id = build_combined_flat(
                db,
                flat_ids=flat_ids,
                filter_=flt,
                binning=binning,
                readout_mode=ro,
                combined_bias_id=bias_id,
                output_rel=str(flat_rel),
            )
            calibs[key] = (bias_id, flat_id)
        else:
            bias_id, flat_id = calibs[key]

        # actual science reduction
        out_path = reduce_science_frame(
            db,
            row,
            combined_bias_id=bias_id,
            combined_flat_id=flat_id,
            out_dir=out_dir,
            mode=cfg.get("reduced_format", "MEF"),
        )
        db.register_reduced_science(
            raw_dp_id=row["dp_id"],
            combined_bias_id=bias_id,
            combined_flat_id=flat_id,
            output_path=out_path,
            processing_version=cfg.get("version", "v0.1.0"),
        )




def main(config_path: str = "config.yaml") -> None:
    cfg = load_config(os.environ['OMEGACAMER_CONFIG'])

    workdir = Path(cfg["working_directory"]).expanduser().resolve()
    os.chdir(workdir)

    db = DatabaseManager(config_path)

    # raw science frames missing reduction
    unreduced = db.conn.execute(
        """
        SELECT * FROM raw_science_files AS s
         WHERE NOT EXISTS (
              SELECT 1 FROM reduced_science_files AS r
               WHERE r.raw_dp_id = s.dp_id
         );
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



if __name__ == "__main__":
    main()
