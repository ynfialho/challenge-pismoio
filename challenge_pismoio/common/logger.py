import logging
from sys import stdout

def settings_logger(name: str = None):
    """
    Applies log settings and returns a logging object.
    :name: logger name
    """
    log_name = name if name else __name__
    logger = logging.getLogger(log_name)
    _= [logger.removeHandler(h) for h in logger.handlers]
    logging.basicConfig(
        level=logging.INFO\
        ,format='%(asctime)s - %(levelname)s - {%(name)s} %(message)s'\
        ,handlers=[logging.StreamHandler(stdout)])
    return logger
