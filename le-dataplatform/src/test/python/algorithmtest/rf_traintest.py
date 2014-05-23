import algorithmtestbase as at
import unittest

class LogisticRegressionTest(unittest.TestCase, at.AlgorithmTestBase):

    def testTrain(self):
        algorithmProperties = { "criterion":"entropy", "n_estimators":"10", "min_samples_split":"25", "min_samples_leaf":"4", "bootstrap": "True" }
        clf = self.execute("rf_train.py", algorithmProperties)
        self.assertTrue(clf != None)

