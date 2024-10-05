#IMPORT LIBRARIES
import logging

#SETTING LOGGING FORMAT
log_format = "%(asctime)s - %(levelname)s - %(message)s"
logger = logging.basicConfig(filename= "app_log.log", level= logging.INFO, filemode= "a",format= log_format,  datefmt= "%d-%m-%Y %H:%M:%S")
logger = logging.getLogger()

def log_into_file(type, msg) :
    if type == "error" :
        logger.error(msg)
    if type == "info" :
        logger.info(msg)
    if type == "critical" :
        logger.critical(msg)
    if type == "debug" :
        logger.debug(msg)
    if type == "warning" :
        logger.warning(msg)