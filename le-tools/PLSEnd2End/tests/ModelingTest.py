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
        marketting_app = PLSEnvironments.pls_marketing_app_ELQ
        
        # Step 4 - Run LoadGroups and activate Model  
        PlsOperations.runModelingLoadGroups(pls_bard, marketting_app);
#         PlsOperations.updateModelingServiceSettings(bardAdminTool);        
#         PlsOperations.activateModel(bardAdminTool,pls_bard);
    
    def testModelingMarketo(self):
        pls_bard = PLSEnvironments.pls_bard_2
        marketting_app = PLSEnvironments.pls_marketing_app_MKTO
        bardAdminTool = PLSEnvironments.pls_bardAdminTool_2
        
        # Step 4 - Run LoadGroups and activate Model  
        PlsOperations.runModelingLoadGroups(pls_bard, marketting_app);
#         PlsOperations.updateModelingServiceSettings(bardAdminTool);        
#         PlsOperations.activateModel(bardAdminTool,pls_bard);
        
    def testModelingSFDC(self):
        pls_bard = PLSEnvironments.pls_bard_3
        marketting_app = PLSEnvironments.pls_marketing_app_SFDC
#         bardAdminTool = PLSEnvironments.pls_bardAdminTool_3
        
        # Step 4 - Run LoadGroups and activate Model  
        PlsOperations.runModelingLoadGroups(pls_bard, marketting_app);
#         PlsOperations.updateModelingServiceSettings(bardAdminTool);        
#         PlsOperations.activateModel(bardAdminTool,pls_bard);


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()