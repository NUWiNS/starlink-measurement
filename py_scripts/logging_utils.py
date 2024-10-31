import logging
import os

logging.basicConfig(
    level=logging.INFO,  # Set the logging level
    format='[%(asctime)s.%(msecs)03d] - %(name)s - %(levelname)s - %(message)s',  # Format of log messages with timezone
    datefmt='%Y-%m-%dT%H:%M:%S',  # Format of the timestamp
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
        filemode: str = 'a',
        level: int = logging.INFO
) -> logging.Logger:
    logger = logging.getLogger(name)
    # Set the logger's level to INFO
    logger.setLevel(level)

    if logger.handlers:
        reset_logger(name)

    if formatter is None:
        formatter = logging.Formatter('[%(asctime)s.%(msecs)03d] - %(name)s - %(levelname)s - %(message)s',
                                      datefmt='%Y-%m-%dT%H:%M:%S')
    
    if filename is not None:
        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))
        file_handler = logging.FileHandler(filename, mode=filemode)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    else:
        # If no filename is provided, we'll use a NullHandler to suppress console output
        logger.addHandler(logging.NullHandler())

    # Disable propagation to prevent logging to console
    logger.propagate = False

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