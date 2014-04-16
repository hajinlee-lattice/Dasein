import algorithmtestbase as at
import leframework.argumentparser as ap
import os
import unittest

class LogisticRegressionTest(unittest.TestCase, at.AlgorithmTestBase):

    def testTrain(self):
        algorithmProperties = { "C":"1.0" }
        self.execute("lr_train.py", algorithmProperties)

