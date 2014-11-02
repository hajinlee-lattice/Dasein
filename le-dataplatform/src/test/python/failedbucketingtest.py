from leframework.executors.learningexecutor import LearningExecutor
from profilingtestbase import ProfilingTestBase


class FailedBucketingTest(ProfilingTestBase):

    def testExecuteProfiling(self):
        from launcher import Launcher
        profilinglauncher = Launcher("badprofile.json")
        profilinglauncher.execute(False)
        learningExecutor = LearningExecutor()

        results = learningExecutor.retrieveMetadata("./results/profile.avro", False)
        self.assertTrue(results is not None)
