#!/usr/local/bin/python
# coding: utf-8

'''
Created on 2015年3月10日

@author: GLiu
'''
import time;
import TestHelpers;
from TestHelpers import DLCRunner;
from TestHelpers import BardAdminRunner;
from Env.Properties import PLSEnvironments;
from BasicOperations.TestHelpers import PLSConfigRunner;
from BasicOperations.TestRunner import SessionRunner;
from BasicOperations.TestHelpers import DLConfigRunner;
from BasicOperations.TestHelpers import PretzelRunner;
from BasicOperations.TestHelpers import JamsRunner;


class Models(object):
    '''
    classdocs
    '''


    def __init__(self, 
                        dlc_host = PLSEnvironments.pls_test_server,
                        dlc_path=PLSEnvironments.dl_dlc_path,
                        dl_server=PLSEnvironments.dl_server,
                        dl_user=PLSEnvironments.dl_server_user,
                        dl_pwd=PLSEnvironments.dl_server_pwd):
        self.dlc_host = dlc_host;
        self.dlc_path = dlc_path;
        self.dl_server = dl_server;
        self.dl_user = dl_user;
        self.dl_pwd = dl_pwd;
        '''
        Constructor
        '''
    def runModelingLoadGroups(self,tenant,marketting_app,
                              step_by_step=False):

        dlc = DLCRunner(host=self.dlc_host, dlc_path=self.dlc_path)
        params = {"-s": self.dl_server,
                  "-u": self.dl_user,
                  "-p": self.dl_pwd,
                  "-t": tenant
                 }
        if marketting_app == PLSEnvironments.pls_marketing_app_ELQ:
            if step_by_step:
                load_groups = ["LoadMapDataForModeling",
                               "LoadCRMDataForModeling",
                               "ModelBuild_PropDataMatch",
                               "CreateEventTableQueries",
                               "CreateAnalyticPlay"]
                assert TestHelpers.runLoadGroups(dlc, params, load_groups)
            else:
                assert TestHelpers.runLoadGroups(dlc, params, ["ExecuteModelBuilding"])
        else:
            # we can't get a method about how to update the nested groups, LoadMapDataForModeling just include the same two sub groups.
            load_groups = ["LoadMAPDataForModeling_ActivityRecord_NewLead",
                           "LoadMAPDataForModeling_LeadRecord",
                           "LoadCRMDataForModeling",
                           "ModelBuild_PropDataMatch",
                           "CreateEventTableQueries",
                           "CreateAnalyticPlay"]
            assert TestHelpers.runLoadGroups(dlc, params, load_groups)

    def updateModelingServiceSettings(self,bard_path):
        # Run AutomaticModelDownloader
        bard = BardAdminRunner(host=self.dlc_host, bard_path=bard_path);
        
        bard.runSetProperty("ModelDownloadSettings", "HDFSNameServerAddress", "10.41.1.216")
        assert bard.getStatus()
        bard.runSetProperty("ModelDownloadSettings", "HDFSNameServerPort", 50070)
        assert bard.getStatus()
        bard.runSetProperty("ScoringConfiguration", "LeadsPerBatch", 10000)
        assert bard.getStatus()
        bard.runSetProperty("ScoringConfiguration", "LeadsPerHour", 50000)
        assert bard.getStatus()
        time.sleep(600);
    
    # Step 4
    # BODCDEPVJOB247.prod.lattice.local
    # Deal with JAMS
    
    # Step 5
    def activateModel(self,bard_path,bard_name=None):
        # Run AutomaticModelDownloader
        bard = BardAdminRunner(host=self.dlc_host, bard_path=bard_path);
        
        "Models available for activate"
        i=1;
        while True:
            bard.runListModels()
            bard.getStatus()
            model_id = bard.getLatestModelId(bard_name);
            if model_id != None or i>15:
                break;
            i+=1;
            print " Can't get the model with bard: %s, will try to get it again after 2 minutes." % bard_name;
            time.sleep(120);
        
        if model_id == None:
            print "We can't get the model which related to the bard: %s" % bard_name;
            assert False;
        else:
            print "Activate Model, the model id is:    ==>    " + model_id;
            bard.runActivateModel(model_id)
            assert bard.getStatus();
      
    def modelingGenerate(self,marketting_app,pls_url):
        
        if pls_url == PLSEnvironments.pls_url_1:
            pls_bard = PLSEnvironments.pls_bard_1;
            bardAdminTool = PLSEnvironments.pls_bardAdminTool_1;
        else:
            pls_bard = PLSEnvironments.pls_bard_2;
            bardAdminTool = PLSEnvironments.pls_bardAdminTool_2;
        # Refresh SVN
        print "Refreshing SVN"
        runner = SessionRunner();
        print runner.runCommandLocally("TortoiseProc /command:update /path:"+PLSEnvironments.svn_location_local+" /closeonend:1")
                   
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
        
        # Step 4 - Run LoadGroups and activate Model  
        self.runModelingLoadGroups(pls_bard, marketting_app);
        self.updateModelingServiceSettings(bardAdminTool);        
        self.activateModel(bardAdminTool,pls_bard);
        print "for jams configurations"
        jams = JamsRunner();
        assert jams.setJamsTenant(pls_bard);
    def modelingGenerateFromDLConfig(self,marketting_app,pls_url):
        if pls_url == PLSEnvironments.pls_url_1:
            pls_bard = PLSEnvironments.pls_bard_1;
            bardAdminTool = PLSEnvironments.pls_bardAdminTool_1;
        else:
            pls_bard = PLSEnvironments.pls_bard_2;
            bardAdminTool = PLSEnvironments.pls_bardAdminTool_2;
        # step 3 - configure dataloader settings
        print "configure dataloader settings"
        dlConfig = DLConfigRunner();
        dlConfig.configDLTables(pls_bard, marketting_app);
        dlConfig.createMockDataProviders(pls_bard, marketting_app);
        dlConfig.editMockRefreshDataSources(pls_bard, marketting_app);
        dlConfig.loadCfgTables(pls_bard, marketting_app);
        
        # Step 4 - Run LoadGroups and activate Model  
        self.runModelingLoadGroups(pls_bard, marketting_app);
        self.updateModelingServiceSettings(bardAdminTool);        
        self.activateModel(bardAdminTool,pls_bard);
        print "for jams configurations"
        jams = JamsRunner();
        assert jams.setJamsTenant(pls_bard);
class Scoring(object):
    '''
    classdocs
    '''
    def __init__(self, tenant,
                        dlc_host = PLSEnvironments.pls_test_server,
                        dlc_path=PLSEnvironments.dl_dlc_path,
                        dl_server=PLSEnvironments.dl_server,
                        dl_user=PLSEnvironments.dl_server_user,
                        dl_pwd=PLSEnvironments.dl_server_pwd):
        self.dlc_host = dlc_host;
        self.dlc_path = dlc_path;
        self.dl_server = dl_server;
        self.dl_user = dl_user;
        self.dl_pwd = dl_pwd;
        self.tenant = tenant;
     
    def waitForLeadInputQueue(self,cycle_times=10,conn=PLSEnvironments.SQL_ScoringDaemon):
        # Wait for the leads to be scored
        dlc = SessionRunner()
        connection_string = conn;
        query = "select * from LeadInputQueue where LEDeployment_ID='%s'" % self.tenant;
        #query = "SELECT * FROM information_schema.tables"
        wait_cycle = 0
        print connection_string
        print query
        while(wait_cycle < cycle_times):
            results = dlc.getQuery(connection_string, query)
            if results:
                #print results
                result = results[-1]
                if len(result) < 3:
                    print "Uknown query result: %s" % result
                    return False;
                else:
                    res = result[-3]
                    print "Status: for %s is %s" % (result[2], res)
                    if res == 2:
                        assert result[-4] == result[-5];
                        print "Job succeeded!"
                        return True;
            print "Sleeping for 180 seconds, hoping Status turns 2"
            wait_cycle += 1
            time.sleep(180)
        return False;
       
    def runScoringGroups(self,load_groups):
        dlc = DLCRunner(host=self.dlc_host, dlc_path=self.dlc_path)
        params = {"-s": self.dl_server,
                  "-u": self.dl_user,
                  "-p": self.dl_pwd,
                  "-t": self.tenant
                 }
        #load_groups = ["LoadCRMData", "LoadMapData", "PropDataMatch", "BulkScoring_PushToScoringDB"];
        return TestHelpers.runLoadGroups(dlc, params, load_groups);
        
    def runPushToBulkScoring(self): 
        load_groups = ["LoadCRMData", "LoadMapData", "PropDataMatch", "BulkScoring_PushToScoringDB"];   
        return self.runScoringGroups(load_groups);
        
    def runPushToHourlyScoring(self):
        load_groups = ["LoadCRMData", "LoadMapData", "PropDataMatch", "PushToScoringDB"];   
        return self.runScoringGroups(load_groups);

    def runEndBulkScoring(self):
        load_groups = ["BulkScoring_PushToLeadDestination"];   
        return self.runScoringGroups(load_groups);

    def runEndHourlyScoring(self):
        load_groups = ["PushToLeadDestination","LoadScoreHistoryData", "PushToReportsDB", "InsightsAllSteps"];   
        return self.runScoringGroups(load_groups);

    def runBulkScoring(self):
        self.runPushToBulkScoring();
        self.waitForLeadInputQueue(cycle_times=100);
        self.runEndBulkScoring();
        
    def runHourlyScoring(self):
        self.runPushToHourlyScoring();
        self.waitForLeadInputQueue(cycle_times=20);
        self.runEndHourlyScoring();

        