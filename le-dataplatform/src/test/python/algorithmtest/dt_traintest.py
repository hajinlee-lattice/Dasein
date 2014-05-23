import algorithmtestbase as at
import unittest

class LogisticRegressionTest(unittest.TestCase, at.AlgorithmTestBase):

    def testTrain(self):
        algorithmProperties = { "criterion":"entropy" }
        clf = self.execute("dt_train.py", algorithmProperties)
        self.assertTrue(clf != None)

