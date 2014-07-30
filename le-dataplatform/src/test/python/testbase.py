import os
from unittest import TestCase

class TestBase(TestCase):

    @classmethod
    def setUpClass(cls):
        curDir = os.path.dirname(os.path.abspath(__file__))
        dataDir = "data/"
        dataDir = os.path.join(curDir,dataDir)
        print dataDir
        for f in os.listdir(dataDir):
            fPath = os.path.join(dataDir,f)
            linkPath = os.path.join(curDir,f)
            if os.path.isfile(fPath) and not os.path.exists(linkPath):
                os.symlink(fPath, linkPath)

    @classmethod
    def tearDownClass(cls):
        curDir = os.path.dirname(os.path.abspath(__file__))
        for f in os.listdir(curDir):
            fPath = os.path.join(curDir,f)
            if os.path.islink(fPath):
                os.unlink(fPath)