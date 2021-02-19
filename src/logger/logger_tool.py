import logging
import os

from teams_logger import TeamsHandler


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
    # Teams Handler
    test_url = os.environ['TEAMS_WEBHOOK_URL']
    teams_handler = TeamsHandler(url=test_url, level=logging.ERROR)
    # Create logger
    logger = logging.getLogger(logger_name)
    logger.addHandler(teams_handler)
    logger.addHandler(log_stream_handler)
    logger.setLevel(level)
    return logger











































