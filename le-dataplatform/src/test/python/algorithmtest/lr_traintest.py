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
        algorithmProperties = { "C":"1.0" }
        clf = self.execute("lr_train.py", algorithmProperties)
        self.assertTrue(clf != None)

