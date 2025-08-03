from astropy.io import fits
import numpy as np

filename = "/scratch/omegacam_work_dir/calib/flats/master_flat_r_SDSS_2024-10-24_normal_1x1.fits"
out_dir = "/scratch/omegacam_work_dir/ccd_masks/"

with fits.open(filename) as hdul:
    for ccd_num, hdu in enumerate(hdul[1:], start=1):
        data = hdu.data.astype(float)
        mask = data <= 0.25  # True (bad) if ≤ 0.25, False (good) if > 0.25
        hdu_mask = fits.PrimaryHDU(mask.astype(np.uint8))  # Save mask as 0/1 image
        hdu_mask.writeto(out_dir + f"{ccd_num}.fits", overwrite=True)
        print(f"Saved mask for CCD {ccd_num} to {ccd_num}.fits")

