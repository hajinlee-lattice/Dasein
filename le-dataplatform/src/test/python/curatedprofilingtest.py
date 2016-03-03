from leframework.executors.learningexecutor import LearningExecutor
from profilingtestbase import ProfilingTestBase


class CuratedProfilingTest(ProfilingTestBase):

    def testExecuteProfiling(self):
        from launcher import Launcher
        profilinglauncher = Launcher("profiledriver-curated.json")
        profilinglauncher.execute(False, postProcessClf=False)
        learningExecutor = LearningExecutor()

        results = learningExecutor.retrieveMetadata("./results/profile.avro", False)
        self.assertTrue(results is not None)
        
         # Check if Skew and Kurtosis are being calculated and added to the Profile.
        self.assertSkewnessAndKurtosis(results)

    def assertSkewnessAndKurtosis(self, results):
        metadataDict = dict(results[0])

        featureKey = "Normal_Distribution"
        self.assertTrue(metadataDict.has_key(featureKey))

        # Check if Kurtosis and Skewness keys are added
        skewnessKey = "skewness"
        self.assertTrue(metadataDict[featureKey][0].has_key(skewnessKey))
        kurtosisKey = "kurtosis"
        self.assertTrue(metadataDict[featureKey][0].has_key(kurtosisKey))

        # Check if Kurtosis and Skewness are calculated for an arbitrary record
        skew = metadataDict[featureKey][0][skewnessKey]
        self.assertEqual(round(skew, 4), 0.1048)

        kurtosis = metadataDict[featureKey][0][kurtosisKey]
        self.assertEqual(round(kurtosis, 4), -1.0554)