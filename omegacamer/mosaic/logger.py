import logging

def setup_logger(log_level=logging.INFO, log_file='omegacam_mosaic.log'):
    logger = logging.getLogger('OmegaCamMosaic')
    logger.setLevel(log_level)

    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler(log_file)

    c_handler.setLevel(log_level)
    f_handler.setLevel(log_level)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(formatter)
    f_handler.setFormatter(formatter)

    logger.addHandler(c_handler)
    logger.addHandler(f_handler)

    return logger

