import algorithmtest as at
import leframework.argumentparser as ap
import unittest

class LogisticRegressionTest(unittest.TestCase, at.AlgorithmBaseTest):

    def testTrain(self):
        self.execute("lr_train.py")

