import os
import shutil
from testbase import TestBase
from algorithmtestbase import AlgorithmTestBase

class DataProfileTest(TestBase, AlgorithmTestBase):

    def setUp(self):
        if os.path.exists("./results"):
            shutil.rmtree("./results")


    def testTrain(self):
        clf = self.execute("data_profile.py", dict(), False)
        self.assertTrue(clf is None)
