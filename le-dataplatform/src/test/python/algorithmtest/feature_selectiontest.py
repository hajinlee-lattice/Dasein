import os
import shutil
from unittest import TestCase
from algorithmtestbase import AlgorithmTestBase

class FeatureSelectionTest(TestCase, AlgorithmTestBase):

    @classmethod
    def setUpClass(cls):
        if os.path.exists("./results"):
            shutil.rmtree("./results")


    def testTrain(self):
        clf = self.execute("feature_selection.py", dict(), False)
        self.assertTrue(clf is None)
