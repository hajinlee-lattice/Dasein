import glob
import json
import os
import sys

from leframework.executors.learningexecutor import LearningExecutor
from trainingtestbase import TrainingTestBase


class ProfilingThenTrainTest(TrainingTestBase):


    def testExecuteProfilingThenTrain(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        # These properties won't really be used since these are just unit tests.
        # Functional and end-to-end tests should be done from java
        profilinglauncher = Launcher("model-badlift-profiling.json")
        profilinglauncher.parser.highUCThreshold = 0.000001
        
        profilinglauncher.execute(False, postProcessClf=False)
        learningExecutor = LearningExecutor()

        results = learningExecutor.retrieveMetadata("./results/profile.avro", False)
        self.assertTrue(results is not None)

        self.assertProfilingData()

        # Check if Skew and Kurtosis are being calculated and added to the Profile.
        self.assertSkewnessAndKurtosis(results)

        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        os.symlink("./results/profile.avro", "profile-badlift.avro")
        traininglauncher = Launcher("model-badlift-training.json")
        traininglauncher.execute(False)

        jsonDict = json.loads(open(glob.glob("./results/*.json")[0]).read())

    def assertProfilingData(self):
        diagnosticsJsonDict = json.loads(open("./results/diagnostics.json").read())
        self.assertEqual(diagnosticsJsonDict["Summary"]["NumberOfSkippedRows"], 0)
        self.assertTrue(len(diagnosticsJsonDict["Summary"]["HighUCColumns"]) > 0)

    def assertSkewnessAndKurtosis(self, results):
        metadataDict = dict(results[0])

        featureKey = "BusinessAnnualSalesAbs"
        self.assertTrue(metadataDict.has_key(featureKey))

        # Check if Kurtosis and Skewness keys are added
        skewnessKey = "skewness"
        self.assertTrue(metadataDict[featureKey][0].has_key(skewnessKey))
        kurtosisKey = "kurtosis"
        self.assertTrue(metadataDict[featureKey][0].has_key(kurtosisKey))

        # Check if Kurtosis and Skewness are calculated for an arbitrary record
        skew = metadataDict[featureKey][0][skewnessKey]
        self.assertEqual(round(skew, 4), 1.5411)

        kurtosis = metadataDict[featureKey][0][kurtosisKey]
        self.assertEqual(round(kurtosis, 4), 0.8976)

    def tearDown(self):
        # Remove launcher module to restore its globals()
        del sys.modules['launcher']

