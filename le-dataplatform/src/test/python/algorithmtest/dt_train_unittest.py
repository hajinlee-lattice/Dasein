import os
import shutil
from testbase import TestBase
from algorithmtestbase import AlgorithmTestBase

class LogisticRegressionTest(TestBase, AlgorithmTestBase):

    def setUp(self):
        if os.path.exists("./results"):
            shutil.rmtree("./results")

    def testTrain(self):
        algorithmProperties = { "criterion":"entropy" }
        clf = self.execute("dt_train.py", algorithmProperties)
        self.assertTrue(clf != None)

