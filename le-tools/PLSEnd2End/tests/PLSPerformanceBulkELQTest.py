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
        
    def testPerformanceBulkEloqua_10K(self):
        pt.PerformanceBulkTest("testPerformanceBulkEloqua_10K",10000)
    
    def testPerformanceBulkEloqua_50K(self):
        pt.PerformanceBulkTest("testPerformanceBulkEloqua_50K",50000)
        
    def testPerformanceBulkEloqua_100K(self):
        pt.PerformanceBulkTest("testPerformanceBulkEloqua_100K",100000)
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()