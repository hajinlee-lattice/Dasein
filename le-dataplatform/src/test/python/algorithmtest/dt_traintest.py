import algorithmtestbase as at
import leframework.argumentparser as ap
import os
import unittest

class LogisticRegressionTest(unittest.TestCase, at.AlgorithmTestBase):

    def testTrain(self):
        algorithmProperties = { "criterion":"entropy" }
        self.execute("dt_train.py", algorithmProperties)

