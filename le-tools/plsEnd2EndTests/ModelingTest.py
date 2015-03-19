'''
Created on Mar 12, 2015

@author: smeng
'''
import unittest
from Properties import PLSEnvironments;
from ServiceRunner.TestHelpers import PLSConfigRunner;
from ServiceRunner.TestRunner import SessionRunner;
from ServiceRunner.TestHelpers import DLConfigRunner;
from ServiceRunner.TestHelpers import PretzelRunner;
from ServiceRunner.TestHelpers import JamsRunner;
from ServiceRunner import PlsOperations



class Test(unittest.TestCase):


    @classmethod
    def setUpClass(cls):
        
        ########## Refresh SVN
        print "Refreshing SVN"
        runner = SessionRunner();
        #TODO -- need to rework considering we are running the tests in jenkins node, not local
        print runner.runCommandLocally("svn update", PLSEnvironments.svn_location_local)
        
        
        ########## setup for Eloqua
        pls_url = PLSEnvironments.pls_url_1
        pls_bard = PLSEnvironments.pls_bard_1
        marketting_app = PLSEnvironments.pls_marketing_app_ELQ
           
        print "Running Setup"
        # Step 1 - Setup Pretzel
        pretzel = PretzelRunner();
        assert pretzel.setupPretzel(marketting_app);
    
        # Step 2 - Configure PLS Credentials        
        print "for PLS Configuration from UI";
        plsUI = PLSConfigRunner(pls_url);
        print "==>    The PLS URL is: %s!" % pls_url;
        plsUI.config(marketting_app);
        
        # step 3 - configure dataloader settings
        print "configure dataloader settings"
        dlConfig = DLConfigRunner();
        dlConfig.configDLTables(pls_bard, marketting_app);
        dlConfig.createMockDataProviders(pls_bard, marketting_app);
        dlConfig.editMockRefreshDataSources(pls_bard, marketting_app);
        dlConfig.loadCfgTables(pls_bard, marketting_app);
        
        
        ########## setup for Marketo
        pls_url = PLSEnvironments.pls_url_2
        pls_bard = PLSEnvironments.pls_bard_2
        marketting_app = PLSEnvironments.pls_marketing_app_MKTO
                   
        print "Running Setup"
        # Step 1 - Setup Pretzel
        pretzel = PretzelRunner();
        assert pretzel.setupPretzel(marketting_app);
    
        # Step 2 - Configure PLS Credentials        
        print "for PLS Configuration from UI";
        plsUI = PLSConfigRunner(pls_url);
        print "==>    The PLS URL is: %s!" % pls_url;
        plsUI.config(marketting_app);
        
        # step 3 - configure dataloader settings
        print "configure dataloader settings"
        dlConfig = DLConfigRunner();
        dlConfig.configDLTables(pls_bard, marketting_app);
        dlConfig.createMockDataProviders(pls_bard, marketting_app);
        dlConfig.editMockRefreshDataSources(pls_bard, marketting_app);
        dlConfig.loadCfgTables(pls_bard, marketting_app);
        



    def testModelingEloqua(self):

        pls_bard = PLSEnvironments.pls_bard_1
        bardAdminTool = PLSEnvironments.pls_bardAdminTool_1
        marketting_app = PLSEnvironments.pls_marketing_app_ELQ
        
        # Step 4 - Run LoadGroups and activate Model  
        PlsOperations.runModelingLoadGroups(pls_bard, marketting_app);
        PlsOperations.updateModelingServiceSettings(bardAdminTool);        
        PlsOperations.activateModel(bardAdminTool,pls_bard);
        print "for jams configurations"
        jams = JamsRunner();
        assert jams.setJamsTenant(pls_bard);
        #TODO -- add more assertions here


    def testModelingMarketo(self):
        pls_bard = PLSEnvironments.pls_bard_2
        marketting_app = PLSEnvironments.pls_marketing_app_MKTO
        bardAdminTool = PLSEnvironments.pls_bardAdminTool_2
        
        # Step 4 - Run LoadGroups and activate Model  
        PlsOperations.runModelingLoadGroups(pls_bard, marketting_app);
        PlsOperations.updateModelingServiceSettings(bardAdminTool);        
        PlsOperations.activateModel(bardAdminTool,pls_bard);
        print "for jams configurations"
        jams = JamsRunner();
        assert jams.setJamsTenant(pls_bard);
        #TODO -- add more assertions here




if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()