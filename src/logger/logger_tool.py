import logging
import os


def get_default_logger(logger_name: str, level: logging = logging.DEBUG):
    '''

    :param logger_name:
    :param slack_channel:
    :param level:
    :return: logging.GetLogger()
    '''
    formatter = logging.Formatter(
        '%(asctime)s - %(filename)s : %(lineno)d line - %(funcName)s - %(levelname)s - %(message)s')

    log_stream_handler = logging.StreamHandler()
    log_stream_handler.setFormatter(formatter)
    # Create logger
    logger = logging.getLogger(logger_name)

    logger.addHandler(log_stream_handler)
    logger.setLevel(level)
    return logger











































