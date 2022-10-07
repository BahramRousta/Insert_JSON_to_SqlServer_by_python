import logging.handlers
import logging


class LogGen:

    @staticmethod
    def loggen():
        logging.basicConfig(filename="Logs/bookStore.log", format='%(asctime)s - %(message)s',
                            datefmt='%Y_%m_%d-%H_%M_%S', filemode='w')
        rotate_file = logging.handlers.RotatingFileHandler(
            "Logs/bookStore.log", maxBytes=1024 * 1024 * 20, backupCount=3
        )
        logger = logging.getLogger()
        logger.addHandler(rotate_file)
        logger.setLevel(logging.INFO)
        return logger

