import logging
import os

logging.basicConfig(
    level=logging.INFO,  # Set the logging level
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
        name: str, 
        filename: str | None = None,
        formatter: logging.Formatter | None = None,
        filemode: str = 'a'
) -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        reset_logger(name)

    if formatter is None:
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                                      datefmt='%Y-%m-%dT%H:%M:%S%z')
    if filename is not None:
        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))
        file_handler = logging.FileHandler(filename, mode=filemode)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    else:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger

class SilentLogger(logging.Logger):
    def __init__(self, name = 'silent'):
        super().__init__(name)

    def log(self, message):
        pass

    def info(self, message):
        pass

    def error(self, message):
        pass

    def debug(self, message):
        pass

    def warning(self, message):
        pass

    def critical(self, message):
        pass

class PrintLogger(logging.Logger):
    def __init__(self, name = 'print'):
        super().__init__(name)

    def log(self, message):
        print(message)

    def info(self, message):
        print(message)

    def error(self, message):
        print(message)

    def debug(self, message):
        print(message)

    def warning(self, message):
        print(message)

    def critical(self, message):
        print(message)