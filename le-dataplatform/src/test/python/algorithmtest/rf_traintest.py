import os
import shutil
from testbase import TestBase
from algorithmtestbase import AlgorithmTestBase

class LogisticRegressionTest(TestBase, AlgorithmTestBase):

    def setUp(self):
        if os.path.exists("./results"):
            shutil.rmtree("./results")

    def testTrain(self):
        algorithmProperties = { "criterion":"entropy", "n_estimators":"10", "min_samples_split":"25", "min_samples_leaf":"4", "bootstrap": "True" }
        clf = self.execute("rf_train.py", algorithmProperties)
        self.assertTrue(clf != None)

