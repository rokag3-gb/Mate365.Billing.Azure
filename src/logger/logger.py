import logging
import os

from src.logger.logger_tool import get_default_logger

log_level = logging.INFO if os.getenv('STAGE') == 'prod' else logging.DEBUG
# log_level = logging.DEBUG
LOGGER = get_default_logger(__name__, level=log_level)