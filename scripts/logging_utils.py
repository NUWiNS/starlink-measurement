import logging
import os

def reset_logger(name):
    logger = logging.getLogger(name)
    # Remove all handlers associated with the logger
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()
    # Also set propagate to False to prevent duplicate logging
    logger.propagate = False

def create_logger(
        name: str, 
        filename: str | None = None,
        formatter: logging.Formatter | None = None,
        filemode: str = 'a',
        level: int = logging.INFO,
        console_output: bool = True
) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    # Prevent propagation to avoid duplicate logging
    logger.propagate = False

    if logger.handlers:
        reset_logger(name)

    if formatter is None:
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                                      datefmt='%Y-%m-%dT%H:%M:%S%z')
    
    # Add file handler if filename is provided
    if filename is not None:
        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))
        file_handler = logging.FileHandler(filename, mode=filemode)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Add console handler if requested or if no filename provided
    if console_output or filename is None:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(level)
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