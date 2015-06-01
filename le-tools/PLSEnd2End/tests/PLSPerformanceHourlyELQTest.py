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
    
    def testPerformanceHourlyEloqua_1K20k3k(self): 
        pt.PerformanceHourlyTest("testPerformanceHourlyEloqua_1K20k3k",1000,"first")
        pt.PerformanceHourlyTest("testPerformanceHourlyEloqua_1K20k3k",20000,"second")
        pt.PerformanceHourlyTest("testPerformanceHourlyEloqua_1K20k3k",3000,"Third")
        
    def testPerformanceHourlyEloqua_20K70k10k(self): 
        pt.PerformanceHourlyTest("testPerformanceHourlyEloqua_20K70k10k",20000,"first")
        pt.PerformanceHourlyTest("testPerformanceHourlyEloqua_20K70k10k",70000,"second")
        pt.PerformanceHourlyTest("testPerformanceHourlyEloqua_20K70k10k",10000,"Third")
        
    def testPerformanceHourlyEloqua_50K50k6Times(self): 
        pt.PerformanceHourlyTest("testPerformanceHourlyEloqua_50K50k6Times",50000,"first")
        pt.PerformanceHourlyTest("testPerformanceHourlyEloqua_50K50k6Times",50000,"2")
        pt.PerformanceHourlyTest("testPerformanceHourlyEloqua_50K50k6Times",50000,"3")
        pt.PerformanceHourlyTest("testPerformanceHourlyEloqua_50K50k6Times",50000,"4")
        pt.PerformanceHourlyTest("testPerformanceHourlyEloqua_50K50k6Times",50000,"5")
        pt.PerformanceHourlyTest("testPerformanceHourlyEloqua_50K50k6Times",50000,"6")
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()