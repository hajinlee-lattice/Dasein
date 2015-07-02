#!/usr/local/bin/python
# coding: utf-8

'''
Created on Mar 16, 2015

@author: GLiu
'''
import time;
import TestHelpers;
from TestHelpers import DLCRunner;
from TestHelpers import BardAdminRunner;
from Properties import PLSEnvironments;
from TestHelpers import PLSConfigRunner;
from TestRunner import SessionRunner;
from TestHelpers import DLConfigRunner;
from TestHelpers import PretzelRunner;
from TestHelpers import JamsRunner;



####################### properties #######################

dlc_host = PLSEnvironments.pls_test_server
dlc_path=PLSEnvironments.dl_dlc_path
dl_server=PLSEnvironments.dl_server
dl_user=PLSEnvironments.dl_server_user
dl_pwd=PLSEnvironments.dl_server_pwd



####################### Modeling operations #######################

def runModelingLoadGroups(tenant,marketting_app,
                          step_by_step=False):

    dlc = DLCRunner(host=dlc_host, dlc_path=dlc_path)
    params = {"-s": dl_server,
              "-u": dl_user,
              "-p": dl_pwd,
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
            assert TestHelpers.runLoadGroups(dlc, params, ["ExecuteModelBuilding"],7200,120)
    elif marketting_app == PLSEnvironments.pls_marketing_app_SFDC:
        if step_by_step:
            load_groups = ["LoadCRMDataForModeling",
                           "ModelBuild_PropDataMatch",
                           "CreateEventTableQueries",
                           "CreateAnalyticPlay"]
            assert TestHelpers.runLoadGroups(dlc, params, load_groups)
        else:
            assert TestHelpers.runLoadGroups(dlc, params, ["ExecuteModelBuilding"],7200,120)
    else:
        # we can't get a method about how to update the nested groups, LoadMapDataForModeling just include the same two sub groups.
        load_groups = ["LoadMAPDataForModeling_ActivityRecord_NewLead",
                       "LoadMAPDataForModeling_LeadRecord",
                       "LoadCRMDataForModeling",
                       "ModelBuild_PropDataMatch",
                       "CreateEventTableQueries",
                       "CreateAnalyticPlay"]
        assert TestHelpers.runLoadGroups(dlc, params, load_groups)

def updateModelingServiceSettings(bard_path):
    # Run AutomaticModelDownloader
    bard = BardAdminRunner(host=dlc_host, bard_path=bard_path);
    
    bard.runSetProperty("ModelDownloadSettings", "HDFSNameServerAddress", "10.41.1.106")
    assert bard.getStatus()
    bard.runSetProperty("ModelDownloadSettings", "HDFSNameServerPort", 14000)
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
def activateModel(bard_path,bard_name=None):
    # Run AutomaticModelDownloader
    bard = BardAdminRunner(host=dlc_host, bard_path=bard_path);
    
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
  
def modelingGenerate(marketting_app,pls_url):
    
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
    runModelingLoadGroups(pls_bard, marketting_app);
    updateModelingServiceSettings(bardAdminTool);        
    activateModel(bardAdminTool,pls_bard);
    print "for jams configurations"
    jams = JamsRunner();
    assert jams.setJamsTenant(pls_bard);
def modelingGenerateFromDLConfig(marketting_app,pls_url):
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
    runModelingLoadGroups(pls_bard, marketting_app);
    updateModelingServiceSettings(bardAdminTool);        
    activateModel(bardAdminTool,pls_bard);
    print "for jams configurations"
    jams = JamsRunner();
    assert jams.setJamsTenant(pls_bard);



####################### Scoring operations #######################
 
def waitForLeadInputQueue(tenant,cycle_times=10,conn=PLSEnvironments.SQL_ScoringDaemon):
    # Wait for the leads to be scored
    dlc = SessionRunner()
    connection_string = conn;
    query = "select * from LeadInputQueue where LEDeployment_ID='%s'" % tenant;
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
#                     assert result[-4] == result[-5];
                    print "Job succeeded!"
                    return True;
        print "Sleeping for 180 seconds, hoping Status turns 2"
        wait_cycle += 1
        time.sleep(180)
    return False;
   
def runScoringGroups(tenant,load_groups):
    dlc = DLCRunner(host=dlc_host, dlc_path=dlc_path)
    params = {"-s": dl_server,
              "-u": dl_user,
              "-p": dl_pwd,
              "-t": tenant
             }
    #load_groups = ["LoadCRMData", "LoadMapData", "PropDataMatch", "BulkScoring_PushToScoringDB"];
    return TestHelpers.runLoadGroups(dlc, params, load_groups);
    
def runPushToBulkScoring(tenant,app=None): 
    print app  
#     if app == PLSEnvironments.pls_marketing_app_ELQ:
#         load_groups = ["LoadMapDataForModeling",
#                        "LoadCRMDataForModeling",
#                        "ModelBuild_PropDataMatch",
#                        "BulkScoring_PushToScoringDB"]            
#     elif app==PLSEnvironments.pls_marketing_app_MKTO:
#         load_groups = ["LoadMAPDataForModeling_ActivityRecord_NewLead",
#                        "LoadMAPDataForModeling_LeadRecord",
#                        "LoadCRMDataForModeling",
#                        "ModelBuild_PropDataMatch",
#                        "BulkScoring_PushToScoringDB"]
#     elif app==PLSEnvironments.pls_marketing_app_SFDC:
#         load_groups = ["LoadCRMDataForModeling",
#                        "ModelBuild_PropDataMatch",
#                        "BulkScoring_PushToScoringDB"]
#     else:
    if app==PLSEnvironments.pls_marketing_app_SFDC:
        load_groups = ["LoadCRMData", "PropDataMatch", "BulkScoring_PushToScoringDB"];
    else:
        load_groups = ["LoadCRMData", "LoadMapData", "PropDataMatch", "BulkScoring_PushToScoringDB"];
        
    return runScoringGroups(tenant, load_groups);
    
def runPushToHourlyScoring(tenant,app=None):
    if app==PLSEnvironments.pls_marketing_app_SFDC:
        load_groups = ["LoadCRMData", "PropDataMatch", "PushToScoringDB"];
    else:
        load_groups = ["LoadCRMData", "LoadMapData", "PropDataMatch", "PushToScoringDB"];
       
    return runScoringGroups(tenant, load_groups);

def runEndBulkScoring(tenant):
    load_groups = ["BulkScoring_PushToLeadDestination"];
    return runScoringGroups(tenant, load_groups);

def runEndHourlyScoring(tenant):
    load_groups = ["PushToLeadDestination"];
    return runScoringGroups(tenant, load_groups);

def runHourlyDanteProcess(tenant):
#     load_groups = ["LoadScoreHistoryData", "PushToReportsDB", "InsightsAllSteps"]; 
    load_groups = ["PushToReportsDB", "InsightsAllSteps"];   
    return runScoringGroups(tenant, load_groups);

def runBulkScoring(tenant,app=None):
    assert runPushToBulkScoring(tenant,app);
    assert waitForLeadInputQueue(tenant, cycle_times=100);
    assert runEndBulkScoring(tenant);
    
def runHourlyScoring(tenant,app=None):
    assert runPushToHourlyScoring(tenant,app);
    assert waitForLeadInputQueue(tenant, cycle_times=20);
    assert runEndHourlyScoring(tenant);
    

if __name__ == '__main__':
    pass