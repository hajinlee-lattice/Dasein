import os
from unittest import TestCase

class TestBase(TestCase):

    @classmethod
    def setUpClass(cls):
        dataDir = "./data/"
        for f in os.listdir(dataDir):
            fPath = os.path.join(dataDir,f)
            if os.path.isfile(fPath) and not os.path.exists(f):
                os.symlink(fPath, f)

    @classmethod
    def tearDownClass(cls):
        curDir = "."
        for f in os.listdir(curDir):
            if os.path.islink(f):
                os.unlink(f)