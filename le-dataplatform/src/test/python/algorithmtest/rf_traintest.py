import algorithmtestbase as at
import leframework.argumentparser as ap
import os
import unittest

class LogisticRegressionTest(unittest.TestCase, at.AlgorithmTestBase):

    def testTrain(self):
        algorithmProperties = { "criterion":"entropy", "n_estimators":"10" }
        self.execute("rf_train.py", algorithmProperties)

