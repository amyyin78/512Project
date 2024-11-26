import logging
import os


class CustomFormatter(logging.Formatter):
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s.%(msecs)03d - %(levelname)s - (%(filename)s:%(lineno)d) - \t%(message)s"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: grey + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, "%Y-%m-%d %H:%M:%S")
        return formatter.format(record)


class LogFactory:
    def __init__(self, name, log_directory, level=logging.DEBUG):
        self.name = name
        self.log_directory = log_directory
        self.log_file = log_directory + f"{name}.log"
        self.level = level
        # NOTE: REMOVE BELOW
        self.level = logging.INFO

    def get_logger(self):
        logger = logging.getLogger(self.name)
        logger.setLevel(self.level)

        ch = logging.StreamHandler()
        ch.setLevel(self.level)
        ch.setFormatter(CustomFormatter())
        logger.addHandler(ch)

        if not os.path.exists(self.log_directory):
            os.makedirs(self.log_directory)

        with open(self.log_file, "w") as file:
            file.write("")

        fh = logging.FileHandler(self.log_file)
        fh.setLevel(self.level)
        fh.setFormatter(CustomFormatter())
        logger.addHandler(fh)

        return logger
