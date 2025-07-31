from typing import Iterable
import numpy as np
from astropy.io import fits
from datetime import datetime, timezone


def build_combined_bias(
    db,
    *,
    bias_ids: Iterable[str],
    binning: str,
    readout_mode: str,
    output_rel: str,
) -> int:
    """
    Create a median bias from the list of *bias_ids*.  Result is written to
    `db.working_directory/output_rel` and registered in the DB.
    Returns the combined_biases.id (existing or new).
    """
    # early-exit if already exists
    output_path = db.working_directory / output_rel
    avg_mjd = np.mean([db.conn.execute("SELECT mjd_obs FROM biases WHERE calib_id=?;",
                                       (bid,)).fetchone()[0] for bid in bias_ids])
    if db.combined_bias_exists(binning=binning, readout_mode=readout_mode,
                               avg_mjd=avg_mjd):
        return db.conn.execute(
            """SELECT id FROM combined_biases
                 WHERE binning=? AND readout_mode=? AND average_mjd_obs=?;""",
            (binning, readout_mode, avg_mjd)
        ).fetchone()[0]

    # discover CCD layout from the first file
    first = db.conn.execute(
        "SELECT file_path FROM biases WHERE calib_id=?;", (next(iter(bias_ids)),)
    ).fetchone()[0]
    with fits.open(first, memmap=True) as hdul:
        n_ccd = len(hdul) - 1
        phdr = hdul[0].header  # keep original primary header

    # create empty output file
    hdul_out = fits.HDUList([fits.PrimaryHDU(header=phdr)])
    for _ in range(n_ccd):
        hdul_out.append(fits.ImageHDU(data=None, header=None))
    hdul_out.writeto(output_path, overwrite=True, output_verify="ignore")

    # combine CCD by CCD so memory ≤ (#bias * one CCD)
    paths = [db.conn.execute(
        "SELECT file_path FROM biases WHERE calib_id=?;", (bid,)
    ).fetchone()[0] for bid in bias_ids]

    for ccd in range(1, n_ccd + 1):
        stack = []
        for p in paths:
            with fits.open(p, memmap=True) as hdul:
                stack.append(hdul[ccd].data.astype("float32"))
        median = np.median(stack, axis=0)
        with fits.open(output_path, mode="update", memmap=False) as h_out:
            h_out[ccd].data = median.astype("float32")

    # register in DB (provenance & metadata)
    scatter = np.std([db.conn.execute("SELECT mjd_obs FROM biases WHERE calib_id=?;",
                                      (bid,)).fetchone()[0] for bid in bias_ids])
    combined_id = db.register_combined_bias(
        binning=binning,
        readout_mode=readout_mode,
        member_calib_ids=bias_ids,
        output_path=output_path,
        avg_mjd=avg_mjd,
        scatter_mjd=scatter,
    )
    return combined_id


# FLAT STACKER  (bias-correct, global normalise, median combine)
def build_combined_flat(
    db,
    *,
    flat_ids: Iterable[str],
    filter_: str,
    binning: str,
    readout_mode: str,
    combined_bias_id: int | None,   # may be NULL
    output_rel: str,
) -> int:
    """
    Make a master flat.  Steps:
      1. subtract bias (if given)
      2. divide each input flat by its own global median
      3. median-combine, CCD by CCD
      4. renormalise final product so global median == 1
    Registers output in DB and returns combined_flats.id.
    """
    output_path = db.working_directory / output_rel

    # fetch bias cube lazily (memmap ccds one by one)
    bias_path = None
    if combined_bias_id is not None:
        bias_path = db.conn.execute(
            "SELECT file_path FROM combined_biases WHERE id=?;",
            (combined_bias_id,)
        ).fetchone()[0]

    first = db.conn.execute(
        "SELECT file_path FROM flats WHERE calib_id=?;", (next(iter(flat_ids)),)
    ).fetchone()[0]
    with fits.open(first, memmap=True) as hdul:
        n_ccd = len(hdul) - 1
        phdr = hdul[0].header

    # prep output
    hdul_out = fits.HDUList([fits.PrimaryHDU(header=phdr)])
    for _ in range(n_ccd):
        hdul_out.append(fits.ImageHDU(data=None, header=None))
    hdul_out.writeto(output_path, overwrite=True, output_verify="ignore")

    # list of paths for fast access
    paths = [db.conn.execute(
        "SELECT file_path FROM flats WHERE calib_id=?;", (fid,)
    ).fetchone()[0] for fid in flat_ids]

    # we need global-median per frame -> store scaling factors
    scales = []

    for fid, p in zip(flat_ids, paths):
        medians = []
        with fits.open(p, memmap=True) as hdul_f:
            for ccd in range(1, n_ccd + 1):
                frame = hdul_f[ccd].data.astype("float32")
                if bias_path:
                    with fits.open(bias_path, memmap=True) as hdul_b:
                        frame -= hdul_b[ccd].data.astype("float32")
                medians.append(np.median(frame))
        scales.append(np.median(medians))  # global median ≈ median of CCD medians

    # now actually stack
    for ccd in range(1, n_ccd + 1):
        stack = []
        for p, scale in zip(paths, scales):
            with fits.open(p, memmap=True) as hdul_f:
                frame = hdul_f[ccd].data.astype("float32")
                if bias_path:
                    with fits.open(bias_path, memmap=True) as hdul_b:
                        frame -= hdul_b[ccd].data.astype("float32")
                stack.append(frame / scale)
        median = np.median(stack, axis=0)
        with fits.open(output_path, mode="update", memmap=False) as h_out:
            h_out[ccd].data = median.astype("float32")

    # final normalisation so combined flat has median==1
    with fits.open(output_path, mode="update", memmap=False) as hdul:
        globals_ = []
        for ccd in range(1, n_ccd + 1):
            globals_.append(np.median(hdul[ccd].data))
        g_med = np.median(globals_)
        for ccd in range(1, n_ccd + 1):
            hdul[ccd].data /= g_med

    # DB registration
    mjds = [db.conn.execute("SELECT mjd_obs FROM flats WHERE calib_id=?;",
                            (fid,)).fetchone()[0] for fid in flat_ids]
    combined_id = db.register_combined_flat(
        filter_=filter_,
        binning=binning,
        readout_mode=readout_mode,
        combined_bias_id=combined_bias_id,
        member_calib_ids=flat_ids,
        output_path=output_path,
        avg_mjd=float(np.mean(mjds)),
        scatter_mjd=float(np.std(mjds)),
    )
    return combined_id


if __name__ == "__main__":
    from database_manager import DatabaseManager
    db = DatabaseManager(config_path='config.yaml')

    night_biases = [row["calib_id"] for row in db.find_biases(
        binning="1x1", readout_mode="normal", mjd_window=0.5, mjd_center=60608.236807)]

    bias_id = build_combined_bias(
        db,
        bias_ids=night_biases,
        binning="1x1",
        readout_mode="normal",
        output_rel=f"calib/master_bias_60608_normal_1x1.fits",
    )

    night_flats = [row["calib_id"] for row in db.find_flats(
        filter_="r_SDSS", binning="1x1", readout_mode="normal",
        mjd_window=0.5, mjd_center=60608.236807, type_="SKY")]

    flat_id = build_combined_flat(
        db,
        flat_ids=night_flats,
        filter_="r_SDSS",
        binning="1x1",
        readout_mode="normal",
        combined_bias_id=bias_id,
        output_rel=f"master_flat_r_60608_normal_1x1.fits",
    )
