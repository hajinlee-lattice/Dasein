'''
Created on Mar 12, 2015

@author: smeng
'''
import unittest
from Properties import PLSEnvironments
from operations.TestHelpers import JamsRunner

from operations import PlsOperations



class Test(unittest.TestCase):


    def testModelingEloqua(self):

        pls_bard = PLSEnvironments.pls_bard_1
        bardAdminTool = PLSEnvironments.pls_bardAdminTool_1
        
        # Step 4 - Run LoadGroups and activate Model  
        PlsOperations.runModelingLoadGroups(pls_bard, marketting_app);
    
    def testModelingMarketo(self):
        pls_bard = PLSEnvironments.pls_bard_2
        marketting_app = PLSEnvironments.pls_marketing_app_MKTO
        
        # Step 4 - Run LoadGroups and activate Model  
        PlsOperations.runModelingLoadGroups(pls_bard, marketting_app);
        
    def testModelingSFDC(self):
        pls_bard = PLSEnvironments.pls_bard_3
        marketting_app = PLSEnvironments.pls_marketing_app_SFDC
        
        # Step 4 - Run LoadGroups and activate Model  
        PlsOperations.runModelingLoadGroups(pls_bard, marketting_app);


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()