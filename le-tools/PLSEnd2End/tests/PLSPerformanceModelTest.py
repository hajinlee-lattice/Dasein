import unittest
from Properties import PLSEnvironments
from operations.TestHelpers import JamsRunner

from operations import PerformanceHelpers
from operations.PerformanceHelpers import PerformanceData
from operations import PlsOperations



class Test(unittest.TestCase):
    
    def testPerformanceModelingEloqua_10K(self):
        self.PerformanceModelingTest("testPerformanceModelingEloqua_10K",10000,PLSEnvironments.pls_bard_1,PLSEnvironments.pls_bardAdminTool_1,PLSEnvironments.pls_marketing_app_ELQ)

    def testPerformanceModelingMKTO_10K(self):
        self.PerformanceModelingTest("testPerformanceModelingMKTO_10K",10000,PLSEnvironments.pls_bard_2,PLSEnvironments.pls_bardAdminTool_2,PLSEnvironments.pls_marketing_app_MKTO)  
        
                
    def PerformanceModelingTest(self,testName,data_number,bard,adminTool,app):
        pls_bard = bard
        bardAdminTool = adminTool
        marketting_app = app
        
        pd = PerformanceData(tenant_name=pls_bard, marketting_app=marketting_app);
        pd.prepareModelBulkData(data_number,'1900-01-01')
       
        # Step 4 - Run LoadGroups and activate Model 
        PerformanceHelpers.createPerformanceDataProviders(pls_bard, marketting_app); 
        PerformanceHelpers.editPerformanceRefreshDataSources(pls_bard, marketting_app); 
                
        PlsOperations.runModelingLoadGroups(pls_bard, marketting_app);
        PlsOperations.updateModelingServiceSettings(bardAdminTool);        
        PlsOperations.activateModel(bardAdminTool,pls_bard);
        print "for jams configurations"
        jams = JamsRunner();
        assert jams.setJamsTenant(pls_bard);
        
        pd.recordPDMatchData(testName,data_number);
        pd.recordLeadScoringData(testName,data_number); 
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()