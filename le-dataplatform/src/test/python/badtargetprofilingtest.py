from leframework.executors.learningexecutor import LearningExecutor
from profilingtestbase import ProfilingTestBase


class BadTargetProfilingTest(ProfilingTestBase):

    def testExecuteProfiling(self):
        from launcher import Launcher
        profilinglauncher = Launcher("bad-target-dataprofile.json")
        profilinglauncher.execute(False)
        learningExecutor = LearningExecutor()

        results = learningExecutor.retrieveMetadata("./results/profile.avro", False)
        
        metadata = results[0]
        
        # Two values for a boolean
        self.assertEquals(len(metadata['IsIT']), 2)
        # Booleans should be categorical
        self.assertEquals(metadata['IsIT'][0]['Dtype'], 'STR')
        self.assertEquals(metadata['IsIT'][1]['Dtype'], 'STR')
        
        self.assertTrue(results is not None)
    

