from subprocess import call
from pathlib import Path


def run_swarp(file_pattern, work_dir, redo, config_file_name, **swarp_config_kwargs):
    """
    run swarp in directory work_dir, the input files must be there.
    :param file_pattern: e.g. "*.fits" to select all fits files.
    :param work_dir: path or str, where the run will take place.
    :param redo: bool, redo even if output exists?
    :param config_file_name: desired name of the swarp config file.
    :param swarp_config_kwargs: passed to swarp_config_kwargs below.
    :return:
    """
    work_dir = Path(work_dir)
    config_file_path = work_dir / config_file_name

    if (work_dir / swarp_config_kwargs['output_filename']).exists() and not redo:
        return
    write_swarp_config(config_file_path=config_file_path, **swarp_config_kwargs)

    # if '*' in file_pattern, we have to expand it here, the shell doesn't handle it
    if '*' in file_pattern:
        file_list = list((work_dir.glob(file_pattern)))
    else:
        file_list = [file_pattern]  # just the one
    call(['swarp'] + file_list + ['-c', config_file_path.name], cwd=str(work_dir))


def write_swarp_config(
    output_filename="coadd.fits",
    weight_output_filename="coadd.weight.fits",
    header_only="N",
    header_suffix=".head",
    weight_type="NONE",
    weight_suffix=".weight.fits",
    combine="Y",
    combine_type="MEDIAN",
    celestial_type="NATIVE",
    projection_type="TAN",
    projection_err=0.001,
    center_type="ALL",
    center_coords="00:00:00.0, +00:00:00.0",
    pixelscale_type="MEDIAN",
    pixel_scale=0.0,
    image_size=0,
    resample="Y",
    resample_dir=".",
    resample_suffix=".resamp.fits",
    resampling_type="LANCZOS3",
    oversampling=0,
    interpolate="N",
    fscale_type="FIXED",
    fscale_keyword="FLXSCALE",
    fscale_default=1.0,
    gain_keyword="GAIN",
    gain_default=0.0,
    subtract_back="N",
    back_type="AUTO",
    back_default=0.0,
    back_size=128,
    back_filtersize=3,
    vmem_dir=".",
    vmem_max=2047,
    mem_max=2048,
    combine_bufsize=256,
    delete_tmpfiles="Y",
    copy_keywords="OBJECT",
    write_fileinfo="N",
    write_xml="Y",
    xml_name="swarp.xml",
    verbose_type="NORMAL",
    nthreads=0,
    config_file_path="swarp.config"
):
    config_content = f"""# Default configuration file for SWarp
#----------------------------------- Output -----------------------------------
IMAGEOUT_NAME          {output_filename}      # Output filename
WEIGHTOUT_NAME         {weight_output_filename} # Output weight-map filename

HEADER_ONLY            {header_only}               # Only a header as an output file (Y/N)?
HEADER_SUFFIX          {header_suffix}           # Filename extension for additional headers

#------------------------------- Input Weights --------------------------------

WEIGHT_TYPE            {weight_type}            # BACKGROUND,MAP_RMS,MAP_VARIANCE or MAP_WEIGHT
WEIGHT_SUFFIX          {weight_suffix}    # Suffix to use for weight-maps

#------------------------------- Co-addition ----------------------------------

COMBINE                {combine}               # Combine resampled images (Y/N)?
COMBINE_TYPE           {combine_type}          # MEDIAN, AVERAGE, MIN, MAX, WEIGHTED, etc.

#-------------------------------- Astrometry ----------------------------------

CELESTIAL_TYPE         {celestial_type}          # NATIVE, PIXEL, EQUATORIAL, etc.
PROJECTION_TYPE        {projection_type}             # WCS projection code or NONE
PROJECTION_ERR         {projection_err}           # Maximum projection error (in pixels)
CENTER_TYPE            {center_type}             # MANUAL, ALL or MOST
CENTER                 {center_coords} # Coordinates of the image center
PIXELSCALE_TYPE        {pixelscale_type}          # MANUAL, FIT, MIN, MAX or MEDIAN
PIXEL_SCALE            {pixel_scale}             # Pixel scale
IMAGE_SIZE             {image_size}               # Image size (0 = AUTOMATIC)

#-------------------------------- Resampling ----------------------------------

RESAMPLE               {resample}               # Resample input images (Y/N)?
RESAMPLE_DIR           {resample_dir}               # Directory path for resampled images
RESAMPLE_SUFFIX        {resample_suffix}    # Filename extension for resampled images
RESAMPLING_TYPE        {resampling_type}        # NEAREST, BILINEAR, LANCZOS2, etc.
OVERSAMPLING           {oversampling}               # Oversampling (0 = automatic)
INTERPOLATE            {interpolate}               # Interpolate bad input pixels (Y/N)?

FSCALASTRO_TYPE        {fscale_type}           # NONE, FIXED, or VARIABLE
FSCALE_KEYWORD         {fscale_keyword}        # FITS keyword for FSCALE
FSCALE_DEFAULT         {fscale_default}             # Default FSCALE value if not in header

GAIN_KEYWORD           {gain_keyword}            # FITS keyword for gain (e-/ADU)
GAIN_DEFAULT           {gain_default}             # Default gain if no FITS keyword found

#--------------------------- Background subtraction ---------------------------

SUBTRACT_BACK          {subtract_back}               # Subtract sky background (Y/N)?
BACK_TYPE              {back_type}            # AUTO or MANUAL
BACK_DEFAULT           {back_default}           # Default background value in MANUAL
BACK_SIZE              {back_size}             # Background mesh size (pixels)
BACK_FILTERSIZE        {back_filtersize}               # Background map filter range (meshes)

#------------------------------ Memory management -----------------------------

VMEM_DIR               {vmem_dir}               # Directory path for swap files
VMEM_MAX               {vmem_max}            # Maximum amount of virtual memory (MB)
MEM_MAX                {mem_max}             # Maximum amount of usable RAM (MB)
COMBINE_BUFSIZE        {combine_bufsize}            # RAM dedicated to co-addition (MB)

#------------------------------ Miscellaneous ---------------------------------

DELETE_TMPFILES        {delete_tmpfiles}               # Delete temporary resampled FITS files (Y/N)?
COPY_KEYWORDS          {copy_keywords}          # List of FITS keywords to propagate
WRITE_FILEINFO         {write_fileinfo}               # Write info about input files in output image header?
WRITE_XML              {write_xml}               # Write XML file (Y/N)?
XML_NAME               {xml_name}       # Filename for XML output
VERBOSE_TYPE           {verbose_type}           # QUIET, LOG, NORMAL, or FULL

NTHREADS               {nthreads}               # Number of simultaneous threads (0 = automatic)
"""
    with open(config_file_path, 'w') as config_file:
        config_file.write(config_content)

