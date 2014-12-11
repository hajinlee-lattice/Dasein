from leframework.executors.learningexecutor import LearningExecutor
from profilingtestbase import ProfilingTestBase


class DP628ProfilingTest(ProfilingTestBase):

    def testExecuteProfiling(self):
        from launcher import Launcher
        profilinglauncher = Launcher("profiledriver-dp628.json")
        profilinglauncher.execute(False)
        learningExecutor = LearningExecutor()

        results = learningExecutor.retrieveMetadata("./results/profile.avro", False)
        self.assertTrue(results is not None)

        metadata = results[0]
        # Two values for a boolean
        #self.assertEquals(len(metadata['MKTOLead_SpamIndicator']), 2)
        # Booleans should be categorical
        #self.assertEquals(metadata['MKTOLead_SpamIndicator'][0]['Dtype'], 'STR')
        #self.assertEquals(metadata['MKTOLead_SpamIndicator'][1]['Dtype'], 'STR')
