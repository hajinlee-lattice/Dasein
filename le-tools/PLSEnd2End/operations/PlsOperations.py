#!/usr/local/bin/python
# coding: utf-8

'''
Created on Mar 16, 2015

@author: GLiu
'''
import time;
import TestHelpers as TestHelpers;
from Properties import PLSEnvironments;
from TestRunner import SessionRunner;

def runModelingLoadGroups(tenant,marketting_app,
                          step_by_step=False):
    if marketting_app == PLSEnvironments.pls_marketing_app_ELQ:
        if step_by_step:
            load_groups = ["LoadMapDataForModeling",
                           "LoadCRMDataForModeling",
                           "ModelBuild_PropDataMatch",
                           "CreateEventTableQueries",
                           "CreateAnalyticPlay"]
            assert TestHelpers.runLoadGroups(tenant, load_groups)
        else:
            assert TestHelpers.runLoadGroup(tenant, "ExecuteModelBuilding", 14400, 120)
    elif marketting_app == PLSEnvironments.pls_marketing_app_SFDC:
        if step_by_step:
            load_groups = ["LoadCRMDataForModeling",
                           "ModelBuild_PropDataMatch",
                           "CreateEventTableQueries",
                           "CreateAnalyticPlay"]
            assert TestHelpers.runLoadGroups(tenant, load_groups)
        else:
            assert TestHelpers.runLoadGroup(tenant, "ExecuteModelBuilding", 14400, 120)
    else:
        # we can't get a method about how to update the nested groups, LoadMapDataForModeling just include the same two sub groups.
        load_groups = ["LoadMAPDataForModeling_ActivityRecord_NewLead",
                       "LoadMAPDataForModeling_LeadRecord",
                       "LoadCRMDataForModeling",
                       "ModelBuild_PropDataMatch",
                       "CreateEventTableQueries",
                       "CreateAnalyticPlay"]
        assert TestHelpers.runLoadGroups(tenant, load_groups)

####################### Scoring operations #######################
 
def waitForLeadInputQueue(tenant,cycle_times=10,conn=PLSEnvironments.SQL_ScoringDaemon):
    # Wait for the leads to be scored
    dlc = SessionRunner()
    connection_string = conn;
    query = "select * from LeadInputQueue where LEDeployment_ID='%s'" % tenant;
    wait_cycle = 0
    print connection_string
    print query
    while(wait_cycle < cycle_times):
        results = dlc.getQuery(connection_string, query)
        if results:
            result = results[-1]
            if len(result) < 3:
                print "Uknown query result: %s" % result
                return False;
            else:
                res = result[-3]
                print "Status: for %s is %s" % (result[2], res)
                if res == 2:
                    print "Job succeeded!"
                    return True;
        print "Sleeping for 180 seconds, hoping Status turns 2"
        wait_cycle += 1
        time.sleep(180)
    return False;
   
def runScoringGroups(tenant,load_groups):
    return TestHelpers.runLoadGroups(tenant, load_groups)


def runPushToBulkScoring(tenant, app=None):
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