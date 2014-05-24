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
        algorithmProperties = { "criterion":"entropy", "n_estimators":"10", "min_samples_split":"25", "min_samples_leaf":"4", "bootstrap": "True" }
        clf = self.execute("rf_train.py", algorithmProperties)
        self.assertTrue(clf != None)

