# logger_config.py
import logging
import graypy
import socket

from logging.handlers import SysLogHandler

"""
    
"""

class ExcludeExternalDebugFilter(logging.Filter):
    def filter(self, record):
        # Permitir todos los mensajes excepto DEBUG de loggers que no sean 'ants_bl'
        if record.levelno == logging.DEBUG and not record.name.startswith('ants_bl'):
            return True
        return True

def setup_logger():
    # Crea un logger
    logger = logging.getLogger('ants_bl')
    logger.setLevel(logging.DEBUG)
    # Si deseas, puedes añadir un handler adicional para mostrar los logs también en consola
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    console_handler.addFilter(ExcludeExternalDebugFilter())
    logger.addHandler(console_handler)


    

    #graylog_handler = SysLogHandler(address=('172.26.211.245', 1514), facility='local0', socktype=socket.SOCK_STREAM)
    graylog_handler = graypy.GELFUDPHandler('localhost', 12201)
    graylog_handler.setLevel(logging.INFO)
    logger.addHandler(graylog_handler)




    return logger

logger = setup_logger() 

"""
level 7: DEBUG
level 6: INFO
level 5: WARNING
level 4: ERROR
level 3: CRITICAL


"""