from algorithmtestbase import AlgorithmTestBase

class LogisticRegressionTest(AlgorithmTestBase):

    def testTrain(self):
        algorithmProperties = "C=1.0"
        clf = self.execute("lr_train.py", algorithmProperties)
        self.assertTrue(clf is not None)

