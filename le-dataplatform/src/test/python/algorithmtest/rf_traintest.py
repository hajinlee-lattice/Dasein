from algorithmtestbase import AlgorithmTestBase

class RandomForestTest(AlgorithmTestBase):

    def testTrain(self):
        algorithmProperties = "criterion=entropy n_estimators=10 min_samples_split=25 min_samples_leaf=4 max_depth=8 bootstrap=true"
        clf = self.execute("rf_train.py", algorithmProperties)
        self.assertTrue(clf is not None)

        # Try with omitted properties
        algorithmProperties = ""
        clf = self.execute("rf_train.py", algorithmProperties)
        self.assertTrue(clf is not None)
