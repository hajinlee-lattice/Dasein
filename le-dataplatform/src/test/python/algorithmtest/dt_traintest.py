from algorithmtestbase import AlgorithmTestBase

class DecisionTreeTest(AlgorithmTestBase):

    def testTrain(self):
        algorithmProperties = "criterion=entropy"
        clf = self.execute("dt_train.py", algorithmProperties)
        self.assertTrue(clf is not None)

