import time
import glob
import json
import sys

import numpy as np

from sklearn import cross_validation
from sklearn import ensemble

from leframework.model.states.crossvalidationgenerator import CrossValidationGenerator
from trainingtestbase import TrainingTestBase

class Timer:
    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start

class CrossValidationTest(TrainingTestBase):

    # Statistical test to make sure, mean and std. deviation of model is correctly calculated
    # 1. Generate data according to random distribution
    # 2. Assign labels randomly, 50% to first class, 50% to second class
    # 3. The expected mean of model accuracy should be 50%
    def testCrossValidation(self):
        self.atestGetMeanStdOfModel()
        self.atestCatchInducedError()
        self.atestEnhancedSummaryFile()
        self.atestCrossValidation()

    def atestGetMeanStdOfModel(self):
        # Generate data and labels
        fake_train_X = np.random.normal(10, 2.1, size=(5000, 5))
        fake_train_Y = np.random.binomial(n=1, p=0.5, size=5000)

        clf = ensemble.RandomForestClassifier(n_estimators=100,
                                          criterion="entropy",
                                          min_samples_split=25,
                                          min_samples_leaf=10,
                                          max_depth=8,
                                          bootstrap=True,
                                          verbose=0)
        clf.fit(fake_train_X, fake_train_Y)

        crossValidationGenerator = CrossValidationStatistics()
        mean, std = crossValidationGenerator.getMeanAndStdOfClf(clf, fake_train_X, fake_train_Y, numberOfFolds = 3)

        expectedMean = 0.5
        expectedDeltaInMean = 0.1 # Give a slack of 10% in the calculated mean
        self.assertAlmostEqual(mean, expectedMean, None , "Cross Validated Mean not within bounds", delta=expectedDeltaInMean)
        expectedStd = 0.01
        expectedDeltaInStd = 0.1 # Give a slack of 10% in the calculated std dev
        self.assertAlmostEqual(std, expectedStd, None , "Cross Validated Std not within bounds", delta=expectedDeltaInStd)

    # Check if error are caught in the functions.Induce an error by sending null classifiers and null data
    def atestCatchInducedError(self):

        enhancedSummaryGenerator = CrossValidationStatistics()
        mean, std = enhancedSummaryGenerator.getMeanAndStdOfClf(classifier= None, features = None, targets= None, numberOfFolds = 3)

        self.assertTrue(mean is None)
        self.assertTrue(std is None)

        mean, std = enhancedSummaryGenerator.getCrossValidationMetrics(mediator = None)

        self.assertTrue(mean is None)
        self.assertTrue(std is None)

    # Check that the Cross Validation Metrics show up in the modelsummary.json
    def atestEnhancedSummaryFile(self):
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        traininglauncher = Launcher("modelCrossValidated.json")
        traininglauncher.execute(False)
        traininglauncher.training

        enhancedJsonDict = json.loads(open(glob.glob("./results/enhancements/modelsummary.json")[0]).read())

        self.assertTrue("CrossValidatedMeanOfModelAccuracy" in enhancedJsonDict)
        self.assertTrue("CrossValidatedStdOfModelAccuracy" in enhancedJsonDict)

        calculatedMean = round(enhancedJsonDict["CrossValidatedMeanOfModelAccuracy"], 4)
        expectedMean = 0.9768
        self.assertAlmostEqual(calculatedMean, expectedMean, None , "Cross Validated Mean not within bounds", delta=0.08)

        calculatedStd = round(enhancedJsonDict["CrossValidatedStdOfModelAccuracy"], 4)
        expectedStd = 0.0005
        self.assertAlmostEqual(calculatedStd, expectedStd, None , "Cross Validated Std not within bounds", delta=0.01)

    # Check that the Mean Model Accuracy with and without Cross-Validation is close to each other
    def atestCrossValidation(self):
        fake_train_X = np.random.uniform(0, 1, size=(5000, 5))
        fake_train_Y = np.random.binomial(n=1, p=0.7, size=5000)

        estimators = 100
        clf = ensemble.RandomForestClassifier(n_estimators=estimators,
                                          criterion="entropy",
                                          min_samples_split=25,
                                          min_samples_leaf=10,
                                          max_depth=8,
                                          bootstrap=True,
                                          verbose=0)
        try:
            with Timer() as t:
                clf.fit(fake_train_X, fake_train_Y)
        finally:
                print 'Time to fit', t.interval

        try:
            with Timer() as t:
                accuracyWithoutCrossValidation = clf.score(fake_train_X, fake_train_Y)

                numberofTimesToTrain = 2
                accuracyWithCrossValidation = cross_validation.cross_val_score( clf, fake_train_X, fake_train_Y, cv=numberofTimesToTrain).mean()

                self.assertAlmostEqual(accuracyWithCrossValidation, accuracyWithoutCrossValidation, None , "Cross Validated Accuracy not close enough to Non-Cross-Validated Accuracy", delta=0.1)
        finally:
                print 'Time', t.interval
