import logging

_logger = logging.getLogger(__name__)


if __name__ == '__main__':
    logFormatter = logging.Formatter("%(asctime)s [%(threadName)s] %(levelname)s %(name)s:%(lineno)d - %(message)s")
    rootLogger = logging.getLogger()
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)
    rootLogger.setLevel(logging.INFO)


