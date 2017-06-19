import logging.config
import os
import yaml


def init_logging(cfg_path=None, env_key='LDC_LOG_CFG'):
    """Setup logging configuration"""
    if cfg_path is None:
        parent = os.path.dirname(os.path.abspath(__file__))
        cfg_path = os.path.join(parent, 'logging.yaml')
    value = os.getenv(env_key, None)
    if value:
        cfg_path = value
    if os.path.exists(cfg_path):
        with open(cfg_path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        fmt = '%(asctime)s [%(threadName)s] %(levelname)s %(name)s:%(lineno)d - %(message)s'
        dfmt = '%Y-%m-%d %H:%M:%S.%s %Z'
        logging.basicConfig(format=fmt, datefmt=dfmt, level=logging.INFO)
