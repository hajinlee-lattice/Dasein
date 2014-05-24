import algorithmtestbase as at
import os
import shutil
import unittest

class LogisticRegressionTest(unittest.TestCase, at.AlgorithmTestBase):

    @classmethod
    def setUpClass(cls):
        if os.path.exists("./results"):
            shutil.rmtree("./results")

    def testTrain(self):
        algorithmProperties = { "criterion":"entropy" }
        clf = self.execute("dt_train.py", algorithmProperties)
        self.assertTrue(clf != None)

