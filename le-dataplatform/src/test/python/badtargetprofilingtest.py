from profilingtestbase import ProfilingTestBase
from leframework.executors.learningexecutor import LearningExecutor


class BadTargetProfilingTest(ProfilingTestBase):

    def testExecuteProfiling(self):
        from launcher import Launcher
        profilinglauncher = Launcher("bad-target-dataprofile.json")
        profilinglauncher.execute(False, postProcessClf=False)
        learningExecutor = LearningExecutor()

        results = learningExecutor.retrieveMetadata("./results/profile.avro", False)
        
        metadata = results[0]
        
        # Only one value for metadata
        self.assertEquals(len(metadata['IsIT']), 1)

        # Booleans are only categorical if the metadata says so
        self.assertEquals(metadata['IsIT'][0]['Dtype'], 'BND')
        
        self.assertTrue(results is not None)
    

