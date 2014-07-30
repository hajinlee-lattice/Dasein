import os
import shutil
from testbase import TestBase
from algorithmtestbase import AlgorithmTestBase

class LogisticRegressionTest(TestBase, AlgorithmTestBase):

    def setUp(self):
        if os.path.exists("./results"):
            shutil.rmtree("./results")

    def testTrain(self):
        algorithmProperties = { "C":"1.0" }
        clf = self.execute("lr_train.py", algorithmProperties)
        self.assertTrue(clf != None)

