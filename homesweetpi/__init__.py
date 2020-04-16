"""Top-level package for HomeSweetPi."""
import os
import logging
from dotenv import load_dotenv

__author__ = """Flann Corcoran"""
__email__ = 'corcorf@posteo.net'
__version__ = '0.1.0'

load_dotenv()

LOG_PATH = os.getenv("LOG_PATH", default="logs")
if not os.path.exists(LOG_PATH):
    os.mkdir(LOG_PATH)


def set_up_python_logging(level="DEBUG",
                          log_filename="homesweetpi.log",
                          log_path=LOG_PATH,
                          name="homesweetpi"):
    """
    Set up the python logging module
    """
    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)

    fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    datefmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt, datefmt=datefmt)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.ERROR)

    log_filename = os.path.join(log_path, log_filename)
    file_handler = logging.FileHandler(log_filename, mode='a')
    file_handler.setFormatter(formatter)
    if level is None or level.upper() not in [
            "DEBUG", "ERROR", "WARNING", "INFO", "CRITICAL"
    ]:
        file_handler.setLevel(logging.DEBUG)
    else:
        file_handler.setLevel(getattr(logging, level.upper()))

    log.addHandler(console_handler)
    log.addHandler(file_handler)
    log.info("Logging level set at %s based on input %s", log.level, level)
    return log


LOG = set_up_python_logging(name="homesweetpi", level="DEBUG",
                            log_filename="homesweetpi.log",
                            log_path=LOG_PATH)
