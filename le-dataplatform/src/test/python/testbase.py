import logging
import os
import re
from unittest import TestCase

logging.basicConfig(level=logging.INFO, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='testbase')

class TestBase(TestCase):

    @classmethod
    def setUpClass(cls):
        logger.info("=========Current test: " + str(cls) + " ===========")
        removeLinks()
        baseDir = os.path.dirname(os.path.abspath(__file__))
        subDir = cls.getSubDir()
        dataDir = os.path.join(baseDir, "data", subDir)
        
        # Creates symbolic links from data directory to current directory
        for f in os.listdir(dataDir):
            fPath = os.path.join(dataDir,f)
            if os.path.isfile(fPath) and not os.path.exists(f):
                os.symlink(fPath, f)

    @classmethod
    def tearDownClass(cls):
        logger.info( "=========Tear down test: " + str(cls) + " ===========")
        removeLinks()
        
    @classmethod
    def getSubDir(cls):
        return ""
    
def removeLinks():
    curDir = "."
    # Removes all symbolic links in current directory
    for f in os.listdir(curDir):
        if os.path.islink(f):
            os.unlink(f)

def removeFiles(curDir):
    removeFilesWithTypes(curDir, [".*.py.gz.*", ".*.p.gz.*", ".*.txt.gz"])

def removeFilesWithTypes(curDir, fileTypes):
    if os.path.exists(curDir):
        # Removes all symbolic links in current directory
        for f in os.listdir(curDir):
            for fileType in fileTypes:
                if re.search(fileType, f):
                    os.remove(os.path.join(curDir, f))

