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
        algorithmProperties = { "criterion":"entropy" }
        clf = self.execute("dt_train.py", algorithmProperties)
        self.assertTrue(clf != None)

