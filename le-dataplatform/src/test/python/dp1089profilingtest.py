import json
from leframework.executors.learningexecutor import LearningExecutor
from profilingtestbase import ProfilingTestBase


class DP1089ProfilingTest(ProfilingTestBase):

    def testExecuteProfiling(self):
        from launcher import Launcher
        profilinglauncher = Launcher("profiledriver-dp1089.json")
        profilinglauncher.execute(False, postProcessClf=False)
        learningExecutor = LearningExecutor()

        results = learningExecutor.retrieveMetadata("./results/profile.avro", False)
        self.assertTrue(results is not None)
        
