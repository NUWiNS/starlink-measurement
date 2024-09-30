import logging
import os

logging.basicConfig(
    level=logging.DEBUG,  # Set the logging level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Format of log messages
    datefmt='%Y-%m-%d %H:%M:%S',  # Format of the timestamp
)


def reset_logger(name):
    logger = logging.getLogger(name)
    # Remove all handlers associated with the logger
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()


def create_logger(
        name: str, filename: str,
        formatter: logging.Formatter = None,
        filemode: str = None
) -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        reset_logger(name)

    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))

    file_handler = logging.FileHandler(filename)
    file_handler.setLevel(logging.INFO)
    if filemode is not None:
        file_handler.mode = filemode
    if formatter is None:
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return logger
