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
        visidb = VisiDBRollBack();        
        visidb.bakVisidbDB(tenantName)
      
    def tearDown(self):
        print "end the test......"
        visidb = VisiDBRollBack();        
        visidb.copyBackVisidbDB(tenantName)
        
    def testPerformanceHourlyEloqua_1K3k5k(self):
        pt.PerformanceHourlyTest("testPerformanceHourlyEloqua_1K3k5k",1000,"first")
        pt.PerformanceHourlyTest("testPerformanceHourlyEloqua_1K3k5k",3000,"second")
        pt.PerformanceHourlyTest("testPerformanceHourlyEloqua_1K3k5k",5000,"Third")
    
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()