from leframework.executors.learningexecutor import LearningExecutor
from profilingtestbase import ProfilingTestBase


class DP628ProfilingTest(ProfilingTestBase):

    def testExecuteProfiling(self):
        from launcher import Launcher
        profilinglauncher = Launcher("profiledriver-dp628.json")
        profilinglauncher.execute(False, postProcessClf=False)
        learningExecutor = LearningExecutor()

        results = learningExecutor.retrieveMetadata("./results/profile.avro", False)
        self.assertTrue(results is not None)

        metadata = results[0]
        # Two values for a boolean
        self.assertEquals(len(metadata['MKTOLead_SpamIndicator']), 2)
        # Booleans should be categorical
        self.assertEquals(metadata['MKTOLead_SpamIndicator'][0]['Dtype'], 'STR')
        self.assertEquals(metadata['MKTOLead_SpamIndicator'][1]['Dtype'], 'STR')
        
        # boolean to STR
        self.assertEquals(len(metadata['MKTOLead_IsFirstLastNameSame']), 2)
        self.assertEqual(metadata['MKTOLead_IsFirstLastNameSame'][0]['Dtype'], 'BND')
        self.assertEqual(metadata['MKTOLead_IsFirstLastNameSame'][1]['Dtype'], 'BND')
        # string to STR
        self.assertEqual(metadata['BusinessECommerceSite'][0]['Dtype'], 'STR')
        
        # int to BND
        self.assertEqual(metadata['CompanyNameLen'][0]['Dtype'], 'BND')
        # float to BND
        self.assertEqual(metadata['FundingAmount'][0]['Dtype'], 'BND')
        # long to BND
        self.assertEqual(metadata['RetirementAssetsYOY'][0]['Dtype'], 'BND')
        
        # String and Interval
        self.assertEqual(metadata['ExperianCreditRating'][0]['Dtype'], 'BND')
        

        
