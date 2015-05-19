import unittest
from Properties import PLSEnvironments
from operations.TestHelpers import JamsRunner

from operations import PerformanceHelpers
from operations.PerformanceHelpers import PerformanceData
from operations.PerformanceHelpers import PerformanceTest
from operations.PerformanceHelpers import VisiDBRollBack
from operations import PlsOperations

revisionId=-1;
tenantName=PLSEnvironments.pls_bard_2;
app=PLSEnvironments.pls_marketing_app_MKTO;
bardAdmin=PLSEnvironments.pls_bardAdminTool_2;
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
    
    def testPerformanceModelingMKTO_20K(self):        
        pt.PerformanceModelingTest("testPerformanceModelingMKTO_20K",20000)  
        
    
    def testPerformanceModelingMKTO_60K(self):        
        pt.PerformanceModelingTest("testPerformanceModelingMKTO_60K",60000)
        
    def testPerformanceModelingMKTO_640K(self):        
        pt.PerformanceModelingTest("testPerformanceModelingMKTO_640K",640000)
        
    def testPerformanceModelingMKTO_1000K(self):        
        pt.PerformanceModelingTest("testPerformanceModelingMKTO_1000K",1000000)
         
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()