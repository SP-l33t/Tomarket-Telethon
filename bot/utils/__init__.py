from .logger import logger, error
from . import launcher, proxy_utils, config_utils


import os

if not os.path.exists(path="sessions"):
    os.mkdir(path="sessions")
