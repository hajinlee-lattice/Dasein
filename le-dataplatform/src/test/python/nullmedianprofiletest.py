from leframework.executors.learningexecutor import LearningExecutor
from profilingtestbase import ProfilingTestBase


class NullMedianProfileTest(ProfilingTestBase):

    def testExecuteProfiling(self):
        from launcher import Launcher
        profilinglauncher = Launcher("profiledriver-okta.json")
        profilinglauncher.execute(False)
        learningExecutor = LearningExecutor()

        results = learningExecutor.retrieveMetadata("./results/profile.avro", False)
        for key in results[0]:
            value = results[0][key]
            for v in value:
                if v["Dtype"] == "BND":
                    self.assertTrue(v["median"] is not None)
                self.assertTrue(v["category"] is not None)
                self.assertTrue(v["displayname"] is not None)
                self.assertTrue(v["approvedusage"] is not None)

