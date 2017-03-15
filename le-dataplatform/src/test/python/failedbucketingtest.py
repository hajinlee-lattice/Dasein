from profilingtestbase import ProfilingTestBase
from leframework.executors.learningexecutor import LearningExecutor


class FailedBucketingTest(ProfilingTestBase):

    def testExecuteProfiling(self):
        from launcher import Launcher
        profilinglauncher = Launcher("badprofile.json")
        profilinglauncher.execute(False, postProcessClf=False)
        learningExecutor = LearningExecutor()

        results = learningExecutor.retrieveMetadata("./results/profile.avro", False)
        self.assertTrue(results is not None)
