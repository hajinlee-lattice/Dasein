from profilingtestbase import ProfilingTestBase


class BadTargetProfilingTest(ProfilingTestBase):

    def testExecuteProfiling(self):
        from launcher import Launcher
        
        from leframework.executors.learningexecutor import LearningExecutor
        profilinglauncher = Launcher("bad-target-dataprofile.json")
        profilinglauncher.execute(False, postProcessClf=False)
        learningExecutor = LearningExecutor()
        
        results = learningExecutor.retrieveMetadata("./results/profile.avro", False)
        
        metadata = results[0]
        
        # Only one value for metadata
        self.assertEquals(len(metadata['IsIT']), 2)

        # Booleans are only categorical if the metadata says so
        self.assertEquals(metadata['IsIT'][0]['Dtype'], 'STR')
        
        self.assertTrue(results is not None)
    

