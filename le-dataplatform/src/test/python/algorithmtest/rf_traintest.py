import os
import shutil
from unittest import TestCase

from algorithmtestbase import AlgorithmTestBase


class LogisticRegressionTest(TestCase, AlgorithmTestBase):

    @classmethod
    def setUpClass(cls):
        if os.path.exists("./results"):
            shutil.rmtree("./results")


    def testTrain(self):
        algorithmProperties = { "criterion":"entropy", "n_estimators":"10", "min_samples_split":"25", "min_samples_leaf":"4", "bootstrap": "True" }
        clf = self.execute("rf_train.py", algorithmProperties)
        self.assertTrue(clf != None)

