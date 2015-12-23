from leframework.executors.learningexecutor import LearningExecutor
from profilingtestbase import ProfilingTestBase

import numpy as np

class ProfilingTest(ProfilingTestBase):

    def testExecuteProfiling(self):
        self.assertSkewNessAndKurtosisWithNullData()

        from launcher import Launcher
        profilinglauncher = Launcher("model-dataprofile.json")
        profilinglauncher.execute(False, postProcessClf=False)
        learningExecutor = LearningExecutor()

        results = learningExecutor.retrieveMetadata("./results/profile.avro", False)
        self.assertTrue(results is not None)

        # Check if Skew and Kurtosis are being calculated and added to the Profile.
        self.assertSkewnessAndKurtosis(results)

    # Kurtosis and Skewness should be none if columnData is empty
    def assertSkewNessAndKurtosisWithNullData(self):
        try:
            fake_train_Y = np.random.binomial(n=1, p=0.5, size=0)
            execfile("data_profile.py", globals())
            skewness, kurtosis = globals()["getKurtosisAndSkewness"](fake_train_Y)

            self.assertTrue(skewness is None)
            self.assertTrue(kurtosis is None)
        except Exception:
            raise Exception('Skewness, Kurtosis Function should return None for bad data')

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
        self.assertEqual(round(skew, 4), 1.4014)

        kurtosis = metadataDict[featureKey][0][kurtosisKey]
        self.assertEqual(round(kurtosis, 4), 0.5726)
