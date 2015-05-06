import unittest
from Properties import PLSEnvironments
from operations.TestHelpers import JamsRunner

from operations import PerformanceHelpers
from operations.PerformanceHelpers import PerformanceData
from operations.PerformanceHelpers import PerformanceTest
from operations.PerformanceHelpers import VisiDBRollBack
from operations import PlsOperations

revisionId=-1;
tenantName=PLSEnvironments.pls_bard_1;
app=PLSEnvironments.pls_marketing_app_ELQ;
bardAdmin=PLSEnvironments.pls_bardAdminTool_1;
pt = PerformanceTest(tenant_name = tenantName,bard_admin = bardAdmin,marketting_app = app);

class Test(unittest.TestCase):    
    
    def setUp(self):
        print "start a new test......"
#         global revisionId
#         visidb = VisiDBRollBack();        
#         revisionId = visidb.dlGetRevision(tenantName)
#         print revisionId
      
    def tearDown(self):
        print "end the test......"
#         visidb = VisiDBRollBack();        
#         visidb.dlRollBack(tenantName,revisionId);
#         print revisionId
        
    def testPerformanceModelingEloqua_20K(self):
        pt.PerformanceModelingTest("testPerformanceModelingEloqua_20K",20000)
    
    def testPerformanceModelingEloqua_60K(self):
        pt.PerformanceModelingTest("testPerformanceModelingEloqua_60K",60000)
    
    def testPerformanceModelingEloqua_640K(self):
        pt.PerformanceModelingTest("testPerformanceModelingEloqua_6400K",640000)
    
    def testPerformanceModelingEloqua_1000K(self):
        pt.PerformanceModelingTest("testPerformanceModelingEloqua_1000K",1000000)    
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()