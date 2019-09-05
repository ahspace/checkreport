import logging.config
import os.path

#Read logging configuration file
LOGGING_CONF=os.path.join(os.path.dirname(__file__), "logging.ini")
logging.config.fileConfig(LOGGING_CONF)