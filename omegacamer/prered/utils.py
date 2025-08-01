from astropy.io import fits
import numpy as np
import importlib.resources
import yaml

with importlib.resources.open_text('omegacamer.prered', 'ccd_overscan.yaml') as f:
    CCD_CROP_BOUNDARIES = yaml.safe_load(f)


def crop_omegacam_overscan(array, ccd_number):
    """
    Crops overscan for a given CCD using boundaries loaded from YAML.
    YAML data is obtained from running this file on a flat. (see main below)

    Parameters
    ----------
    array : 2D numpy array
        Image array to crop.
    ccd_number : int
        CCD number (must match keys in the YAML).

    Returns
    -------
    cropped_array : 2D numpy array
        Cropped array according to the CCD boundaries.
    """
    try:
        r0, r1, c0, c1 = CCD_CROP_BOUNDARIES[ccd_number]
    except KeyError:
        raise ValueError(f"CCD number {ccd_number} not found in crop boundaries.")
    return array[r0:r1, c0:c1]


def find_overscan_edges(array, bias_level=250, flat_level=30000, axis=0, threshold=0.5):
    """
    Finds where the overscan transitions to the illuminated region along the given axis.
    axis=0: scan rows (for top/bottom), axis=1: scan columns (for left/right)
    Returns: (low_index, high_index) to crop, inclusive
    """
    # median across other axis to get 1D profile
    profile = np.median(array, axis=1-axis)
    # threshold between bias and flat
    cut = (bias_level + flat_level) / 2 * threshold
    mask = profile > cut
    # find first and last True
    good = np.where(mask)[0]
    return int(good[0]), int(good[-1]+1)

def find_overscan_edges_from_flat_file(filename):

    with fits.open(filename) as hdul:
        print("Extension | y_min:y_max, x_min:x_max")
        crop_dict = {}
        for ext in range(1, len(hdul)):  # skipping primary
            arr = hdul[ext].data.astype(float)
            y0, y1 = find_overscan_edges(arr, axis=0)
            x0, x1 = find_overscan_edges(arr, axis=1)
            print(f"{ext:9d} : {y0}:{y1}, {x0}:{x1}")
            crop_dict[ext] = [y0, y1, x0, x1]
    print()
    yaml_str = yaml.dump(crop_dict, sort_keys=True)
    print(yaml_str)
    return crop_dict


if __name__ == "__main__":
    from sys import argv
    find_overscan_edges_from_flat_file(argv[1])
