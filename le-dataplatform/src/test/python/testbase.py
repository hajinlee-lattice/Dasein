import logging
import os
from unittest import TestCase


logging.basicConfig(level=logging.INFO, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='testbase')

class TestBase(TestCase):

    @classmethod
    def setUpClass(cls):
        logger.info("=========Current test: " + str(cls) + " ===========")
        baseDir = os.path.dirname(os.path.abspath(__file__))
        dataDir = baseDir+ "/data/"
        # Creates symbolic links from data directory to current directory
        for f in os.listdir(dataDir):
            fPath = os.path.join(dataDir,f)
            if os.path.isfile(fPath) and not os.path.exists(f):
                os.symlink(fPath, f)

    @classmethod
    def tearDownClass(cls):
        logger.info( "=========Tear down test: " + str(cls) + " ===========")
        curDir = "."
        # Removes all symbolic links in current directory
        for f in os.listdir(curDir):
            if os.path.islink(f):
                os.unlink(f)

