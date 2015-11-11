#!/usr/local/bin/python
# coding: utf-8

# import modules
# import modules
import threading
import datetime
import requests
import json
import time
import base64
import random
import string
import os.path
from selenium import webdriver
import thread

import TestHelpers;
from Properties import PLSEnvironments
from operations.TestRunner import SessionRunner
from operations.TestHelpers import DLCRunner
from operations import PlsOperations

test_connection_string = PLSEnvironments.SQL_BasicDataForIntegrationTest;
dl_connection_string = PLSEnvironments.SQL_conn_dataloader;
pd_connection_string = PLSEnvironments.SQL_conn_pdMatch;
ld_connection_string = PLSEnvironments.SQL_conn_leadscoring;
pls_connection_string = PLSEnvironments.SQL_ScoringDaemon;
testThreadCount = 0
testThreadLock = thread.allocate_lock()
performanceRunID = -1
perfTable_GroupRunData = PLSEnvironments.perfTable_GroupRunData


def createPerformanceDataProviders(tenant, marketting_app, host=PLSEnvironments.pls_test_server,
                                   dlc_path=PLSEnvironments.dlc_path,
                                   dl_server=PLSEnvironments.dl_server,
                                   user=PLSEnvironments.dl_server_user,
                                   password=PLSEnvironments.dl_server_pwd):
    dlc = DLCRunner(host=host, dlc_path=dlc_path)
    command = "New Data Provider"
    params = {"-s": dl_server,
              "-u": user,
              "-p": password,
              "-t": tenant,
              "-dpf": '"upload|validation extract|leaf extract|itc|fstable"',
              "-v": "true"
              }
    # Mock SFDC
    if "ELQ" == marketting_app:
        params["-cs"] = '"%s"' % PLSEnvironments.performance_ELQ_SFDC_DataProvider;
        params["-dpn"] = "performance_SFDC_DataProvider";
    elif "MKTO" == marketting_app:
        params["-cs"] = '"%s"' % PLSEnvironments.performance_MKTO_SFDC_DataProvider;
        params["-dpn"] = "performance_SFDC_DataProvider";

    params["-dpt"] = "sqlserver"
    print "Performnce Mock SFDC"
    dlc.runDLCcommand(command, params)
    dlc.getStatus()

    # Mock Marketting App
    if "ELQ" == marketting_app:
        params["-cs"] = '"%s"' % PLSEnvironments.performance_ELQ_ELQ_DataProvider;
        params["-dpn"] = "performance_ELQ_ELQ_DataProvider";
    elif "MKTO" == marketting_app:
        params["-cs"] = '"%s"' % PLSEnvironments.performance_MKTO_MKTO_DataProvider;
        params["-dpn"] = "performance_MKTO_MKTO_DataProvider";

    params["-dpt"] = "sqlserver"
    print "Performance Mock %s" % marketting_app
    dlc.runDLCcommand(command, params)
    dlc.getStatus()


def editPerformanceRefreshDataSources(tenant, marketting_app, host=PLSEnvironments.pls_test_server,
                                      dlc_path=PLSEnvironments.dlc_path,
                                      dl_server=PLSEnvironments.dl_server,
                                      user=PLSEnvironments.dl_server_user,
                                      password=PLSEnvironments.dl_server_pwd):
    dlc = DLCRunner(host=host, dlc_path=dlc_path)
    command = "Edit Refresh Data Source"
    params = {"-s": dl_server,
              "-u": user,
              "-p": password,
              "-t": tenant
              }

    # LoadCRMDataForModeling
    rds_list = ["SFDC_User", "SFDC_Contact", "SFDC_Lead", "SFDC_Opportunity", "SFDC_OpportunityContactRole"]
    for rds in rds_list:
        params["-g"] = "LoadCRMDataForModeling"
        params["-rn"] = rds
        params["-cn"] = "performance_SFDC_DataProvider"
        print "Updating Refresh Data Source %s for performance_SFDC_DataProvider" % rds
        dlc.runDLCcommand(command, params)
        # dlc.getStatus()

    # LoadMAPDataForModeling
    if marketting_app == "ELQ":
        params["-g"] = "LoadMAPDataForModeling"
        params["-rn"] = "ELQ_Contact"
        params["-cn"] = "performance_ELQ_ELQ_DataProvider";
        print "Updating Refresh Data Source ELQ_Contact for performance_ELQ_ELQ_DataProvider"
        dlc.runDLCcommand(command, params)
        # print dlc.getStatus()


    # "LoadMAPDataForModeling_ActivityRecord_OtherThanNewLead": "ActivityRecord_OtherThanNewLead",
    elif marketting_app == "MKTO":
        params["-cn"] = "performance_MKTO_MKTO_DataProvider"
        rds_dict = {"LoadMAPDataForModeling_ActivityRecord_NewLead": "ActivityRecord_NewLead",
                    "LoadMAPDataForModeling_LeadRecord": "MKTO_LeadRecord"}
        for lg in rds_dict:
            params["-g"] = lg
            params["-rn"] = rds_dict[lg]
            print "Updating Refresh Data Source %s for performance_Marketo_Data_Provider" % rds_dict[lg]
            dlc.runDLCcommand(command, params)
            # dlc.getStatus()
    else:
        print "!!![%s] MARKETTING UP IS NOT SUPPORTED!!!" % marketting_app


def editHourlyPerformanceRefreshDataSources(tenant, marketting_app, host=PLSEnvironments.pls_test_server,
                                            dlc_path=PLSEnvironments.dlc_path,
                                            dl_server=PLSEnvironments.dl_server,
                                            user=PLSEnvironments.dl_server_user,
                                            password=PLSEnvironments.dl_server_pwd):
    dlc = DLCRunner(host=host, dlc_path=dlc_path)
    command = "Edit Refresh Data Source"
    params = {"-s": dl_server,
              "-u": user,
              "-p": password,
              "-t": tenant
              }

    # LoadCRMDataForModeling
    rds_list = ["SFDC_User", "SFDC_Contact", "SFDC_Lead", "SFDC_Opportunity", "SFDC_OpportunityContactRole"]
    for rds in rds_list:
        params["-g"] = "LoadCRMData"
        params["-rn"] = rds
        params["-cn"] = "performance_SFDC_DataProvider"
        print "Updating Refresh Data Source %s for performance_SFDC_DataProvider" % rds
        dlc.runDLCcommand(command, params)
        # dlc.getStatus()

    # LoadMAPDataForModeling
    if marketting_app == "ELQ":
        params["-cn"] = "performance_ELQ_ELQ_DataProvider";
        rds_dict = {"LoadMAPData_ModLeads": "ELQ_Contact",
                    "LoadMAPData_NewLeads": "ELQ_Contact"}
        for lg in rds_dict:
            params["-g"] = lg
            params["-rn"] = rds_dict[lg]
            print "Updating Refresh Data Source ELQ_Contact for performance_ELQ_ELQ_DataProvider"
            dlc.runDLCcommand(command, params)
            # print dlc.getStatus()


    # "LoadMAPDataForModeling_ActivityRecord_OtherThanNewLead": "ActivityRecord_OtherThanNewLead",
    elif marketting_app == "MKTO":
        params["-cn"] = "performance_MKTO_MKTO_DataProvider"
        rds_dict = {"LoadMAPData_ActivityRecord_MissingActivity": "ActivityRecord_MissingActivity",
                    "LoadMAPData_ActivityRecord_NewLead": "ActivityRecord_NewLead",
                    "LoadMAPData_ActivityRecord_RecentActivity": "ActivityRecord_RecentActivity",
                    #                         "LoadMAPData_ActivityRecord_RecentActivity_Ext":"",
                    "LoadMAPData_LeadRecord": "LeadRecord"}
        for lg in rds_dict:
            params["-g"] = lg
            params["-rn"] = rds_dict[lg]
            print "Updating Refresh Data Source %s for performance_Marketo_Data_Provider" % rds_dict[lg]
            dlc.runDLCcommand(command, params)
            # dlc.getStatus()
    else:
        print "!!![%s] MARKETTING UP IS NOT SUPPORTED!!!" % marketting_app


def getSequence():
    dlc = SessionRunner()
    query = "select max(sequence) from performance_time_cost";
    result = dlc.getQuery(test_connection_string, query);
    if None == result[0][0]:
        return 0;
    else:
        return result[0][0] + 1;


def getPerformanceRunID():
    # if run id not initialized, initialize it, else return it
    global performanceRunID
    if performanceRunID == -1:
        try:
            sr = SessionRunner()
            query = "select max(RunID) as runid from %s" % perfTable_GroupRunData;
            result = sr.getQuery(test_connection_string, query);
            performanceRunID = 0 if result[0]['runid'] == None else result[0]['runid'] + 1
            print "Performance run id: ", performanceRunID
        except Exception:
            print 'Get last performance run ID failed from %s' % test_connection_string
    return performanceRunID


def getLaunchPerfData(dl_connection_string, lauchid):
    query = "SELECT l.LaunchId,l.GroupName,l.TenantId,l.[IsNestedLaunch],l.[ParentNestedLaunchId],l.[Status],l.[CreateTime],l.[ValidateStart],l.[ValidateEnd],l.[GenerateBCPStart],l.[GenerateBCPEnd],l.[LaunchStart],l.[LaunchEnd]" + \
            " FROM [DataLoader].[dbo].[Launches] l" + \
            " where l.LaunchId=%s" % lauchid
    sr = SessionRunner()
    result = sr.getQuery(dl_connection_string, query)
    if None == result:
        print "========>there is no data be found for conn: %s" % dl_connection_string;
        print "========>the query is: %s" % query;
    else:
        isNestGroup = result[0].get("isnestedlaunch")
        if (isNestGroup):
            query2 = "SELECT l.[LaunchId],l.[GroupName],l.[TenantId],l.[IsNestedLaunch],l.[ParentNestedLaunchId],l.[Status],l.[CreateTime],l.[ValidateStart],l.[ValidateEnd],l.[GenerateBCPStart],l.[GenerateBCPEnd],l.[LaunchStart],l.[LaunchEnd]" + \
                     " FROM [DataLoader].[dbo].[Launches] l" + \
                     " where l.[ParentNestedLaunchId]=%s" % lauchid
            result = sr.getQuery(dl_connection_string, query2)
    return result;


def recordLaunchPerfData(perf_db_connection, tenant, loadGroup, launchData):
    runid = getPerformanceRunID()
    totalCost = 0
    if not launchData:
        print "The Launch Data is None"
        return
    for row in launchData:
        try:
            validate_start = row["validatestart"].strftime('%Y-%m-%d %X') if row["validatestart"] else 'null'
            validate_end = row["validateend"].strftime('%Y-%m-%d %X') if row["validateend"] else 'null'
            validate_duration = (row["validateend"] - row["validatestart"]).seconds if (
                row["validateend"] and row["validatestart"]) else 0
            gbcp_start = row["generatebcpstart"].strftime('%Y-%m-%d %X') if row["generatebcpstart"] else 'null'
            gbcp_end = row["generatebcpend"].strftime('%Y-%m-%d %X') if row["generatebcpend"] else 'null'
            gbcp_duration = (row["generatebcpend"] - row["generatebcpstart"]).seconds if (
                row["generatebcpend"] and row["generatebcpstart"]) else 0
            launch_start = row["launchstart"].strftime('%Y-%m-%d %X') if row["launchstart"] else 'null'
            launch_end = row["launchend"].strftime('%Y-%m-%d %X') if row["launchend"] else 'null'
            launch_duration = (row["launchend"] - row["launchstart"]).seconds if (
                row["launchend"] and row["launchstart"]) else 0
            group_duration = (row["launchend"] - row["validatestart"]).seconds if (
                row["launchend"] and row["validatestart"]) else 0
            print "Record Perf Data: ", tenant, row["groupname"]
            query = "INSERT INTO [%s]" % PLSEnvironments.perfTable_GroupRunData + \
                    "VALUES( %d,%d,%s,%d,'%s','%s','%s','%s','%s','%s',%d,'%s','%s',%d,'%s','%s',%d,%d,'%s') " % (
                        runid, row["launchid"],
                        row["parentnestedlaunchid"] if row["parentnestedlaunchid"] else 'null', row["status"],
                        tenant, row["groupname"], loadGroup, row["isnestedlaunch"],
                        validate_start, validate_end, validate_duration,
                        gbcp_start, gbcp_end, gbcp_duration,
                        launch_start, launch_end, launch_duration,
                        group_duration, datetime.datetime.utcnow().strftime('%Y-%m-%d %X')
                    )
            print query
            totalCost += group_duration
            sr = SessionRunner()
            sr.execQuery(perf_db_connection, query)
        except Exception as e:
            print "Write performance data to test database failed: %s" % e.message
    else:
        print "Total cost of load group %s: %s s" % (loadGroup, totalCost)


def perf_waitForAllThreadFinished(interval=PLSEnvironments.perf_CheckInterval):
    interval = int(interval) * 2
    while testThreadCount > 0:
        print "Test thread count: %d, wait for another %ss" % (testThreadCount, interval)
        time.sleep(interval)
    print 'All thread finished'


def runLoadGroupsWithPerfCounter(tenant, load_groups, max_run_time_in_sec=10800,
                                 sleep_time=int(PLSEnvironments.perf_CheckInterval)):
    for loadGroup in load_groups:
        succeed, launchid = TestHelpers.runLoadGroup(tenant, loadGroup, max_run_time_in_sec, sleep_time)
        if succeed:
            launchData = getLaunchPerfData(dl_connection_string, launchid)
            recordLaunchPerfData(test_connection_string, tenant, loadGroup, launchData)
        else:
            print 'Run failed load group at: %s, Launch id: %s' % (loadGroup, launchid)
            break
    return succeed


class PerformanceTestRunner(threading.Thread):
    def __init__(self, tenant, loadGroups):
        threading.Thread.__init__(self)
        self.tenant = tenant
        self.loadGroups = loadGroups
        self.runid = getPerformanceRunID()

    def run(self):
        global testThreadCount, testThreadLock, performanceRunID
        testThreadLock.acquire
        testThreadCount += 1
        testThreadLock.release

        try:
            runLoadGroupsWithPerfCounter(self.tenant, self.loadGroups)
        except Exception, e:
            print "Exception happened in thread: %s" % self.name
            print e.message

        testThreadLock.acquire
        testThreadCount -= 1
        testThreadLock.release


class PerformanceData(SessionRunner):
    def __init__(self, marketting_app, tenant_name=None, host=PLSEnvironments.pls_test_server, logfile=None,
                 exception=False):
        super(PerformanceData, self).__init__(host, logfile)
        self.exception = exception
        self.app = marketting_app;
        self.tenantName = tenant_name;
        self.sequence = getSequence();

    def recordDanteData(self, test_Name, record_number, groupName):
        dllaunchData = self.getLaunchData(groupName);
        if -1 == dllaunchData:
            return 0;
        print "record Dante Data, launch data: "
        print dllaunchData.get("launchid");
        dlDTDatas = self.getDanteData_dataloader(dllaunchData.get("launchid"));
        if 0 == len(dlDTDatas):
            print "failed to get dante data from dataloader";
            return 0;
        dlDTData = dlDTDatas[0]
        print "record Dante Data, Dataloader data:"
        print dlDTData
        dtDatas = self.getDanteData(dlDTData.get("starttime"));
        if 0 == len(dtDatas):
            print "failed to get dante data from dante";
            return 0;
        dtData = dtDatas[0]
        print "record Dante Date, Dante: "
        print dtData
        print self.performanceDataRecord(sequence=self.sequence, testingName=test_Name, loadgroupName=groupName,
                                         serviceName=pls_connection_string, recordNumber=record_number,
                                         dl_launch_begin_date=dllaunchData.get("validatestart"),
                                         dl_launch_end_date=dllaunchData.get("launchend"),
                                         dl_launch_duration_time=dllaunchData.get("dl_launch_duration_time"),
                                         dl_begin_date=dlDTData.get("starttime"), dl_end_date=dlDTData.get("endtime"),
                                         dl_duration_time=dlDTData.get("dl_duration_time"),
                                         begin_date=dtData.get("starttime"), end_date=dtData.get("endtime"),
                                         duration_time=dtData.get("duration"), actuall_number=dtData.get("num_rows"))

    def recordBardInData(self, test_Name, record_number, groupName):
        dllaunchData = self.getLaunchData(groupName);
        if -1 == dllaunchData:
            return 0;
        print "record BardIn Data, launch data: "
        print dllaunchData.get("launchid");
        dlBIDatas = self.getBardInData_dataloader(dllaunchData.get("launchid"));
        if 0 == len(dlBIDatas):
            print "failed to get BardIn data from dataloader";
            return 0;
        dlBIData = dlBIDatas[0]
        print "record BardIn Data, Dataloader BardIn: "
        print dlBIData.get("queryname")
        biDatas = self.getBardInData(dlBIData.get("queryname"), dllaunchData.get("launchid"));
        if 0 == len(biDatas):
            print "failed to get BardIn data from BardService";
            return 0;
        biData = biDatas[0]
        print "record BardIn Data, bardin data:"
        print biData;
        print self.performanceDataRecord(sequence=self.sequence, testingName=test_Name, loadgroupName=groupName,
                                         serviceName=pls_connection_string, recordNumber=record_number,
                                         dl_launch_begin_date=dllaunchData.get("validatestart"),
                                         dl_launch_end_date=dllaunchData.get("launchend"),
                                         dl_launch_duration_time=dllaunchData.get("dl_launch_duration_time"),
                                         dl_begin_date=dlBIData.get("starttime"), dl_end_date=dlBIData.get("endtime"),
                                         dl_duration_time=dlBIData.get("dl_duration_time"),
                                         begin_date=biData.get("starttime"), end_date=biData.get("endtime"),
                                         duration_time=biData.get("duration"), QueryName=dlBIData.get("queryname"),
                                         actuall_number=biData.get("total"))

    def recordLeadScoringData(self, test_Name, record_number):
        dllaunchData = self.getLaunchData("CreateAnalyticPlay");
        if -1 == dllaunchData:
            return 0;
        print "record LeadScoring Data, launch data: "
        print dllaunchData.get("launchid");
        dlLDDatas = self.getLeadScoringData_dataloader(dllaunchData.get("launchid"));
        if 0 == len(dlLDDatas):
            print "failed to get leadScoring data from dataloader";
            return 0;
        dlLDData = dlLDDatas[0];
        print "record LeadScoring Data, dataloader Lead Scoring:"
        print dlLDData.get("commandid")
        ldDatas = self.getLeadScoringData(dlLDData.get("commandid"));
        if 0 == len(ldDatas):
            print "failed to get LeadScoring data from LeadScoring service";
            return 0;
        ldData = ldDatas[0]
        print "record LeadScoring Data, leadScoring data:"
        print ldData;
        print self.performanceDataRecord(sequence=self.sequence, testingName=test_Name,
                                         loadgroupName="CreateAnalyticPlay",
                                         serviceName=ld_connection_string, recordNumber=record_number,
                                         dl_launch_begin_date=dllaunchData.get("validatestart"),
                                         dl_launch_end_date=dllaunchData.get("launchend"),
                                         dl_launch_duration_time=dllaunchData.get("dl_launch_duration_time"),
                                         dl_begin_date=dlLDData.get("starttime"), dl_end_date=dlLDData.get("endtime"),
                                         dl_duration_time=dlLDData.get("dl_duration_time"),
                                         begin_date=ldData.get("starttime"), end_date=ldData.get("endtime"),
                                         duration_time=ldData.get("duration"))

    def recordPDMatchData(self, test_Name, record_number, pdMatchName):
        dllaunchData = self.getLaunchData(pdMatchName);
        if -1 == dllaunchData:
            return 0;
        print "record PDMatch Data, launch data: "
        print dllaunchData.get("launchid");
        dlPDDatas = self.getPDMatchData_dataloader(dllaunchData.get("launchid"));
        if 0 == len(dlPDDatas):
            print "failed to get PDMatch data from dataloader";
            return 0;
        print "record PDMatch Data, dataloader PDMatch: "
        dlPDData = dlPDDatas[0]
        print dlPDData.get("commandid");
        pdDatas = self.getPDMatchData(dlPDData.get("commandid"));
        if 0 == len(pdDatas):
            print "failed to get PDMatch data from PropDataMatchService";
            return 0;
        pdData = pdDatas[0]
        print "record PDMatch Data, pdMatch :"
        print pdData;
        print self.performanceDataRecord(sequence=self.sequence, testingName=test_Name, loadgroupName=pdMatchName,
                                         serviceName=pd_connection_string, recordNumber=record_number,
                                         dl_launch_begin_date=dllaunchData.get("validatestart"),
                                         dl_launch_end_date=dllaunchData.get("launchend"),
                                         dl_launch_duration_time=dllaunchData.get("dl_launch_duration_time"),
                                         dl_begin_date=dlPDData.get("starttime"), dl_end_date=dlPDData.get("endtime"),
                                         dl_duration_time=dlPDData.get("dl_duration_time"),
                                         begin_date=pdData.get("starttime"), end_date=pdData.get("endtime"),
                                         duration_time=pdData.get("duration"), actuall_number=pdData.get("num_rows"))

    def getDanteData(self, starttime):
        query = "select count(last_modification_date) as num_rows,convert(varchar(19),min(last_modification_date),120) as starttime, convert(varchar(19),max(last_modification_date),120) as endtime, datediff(s,min(last_modification_date),max(last_modification_date)) as duration " + \
                " from (select last_modification_date from [%s].[dbo].[LeadCache] where datediff(s,last_modification_date,'%s')<0) t" % (
                    PLSEnvironments.pls_db_Dante, starttime)
        #         print query
        result = self.getQuery(pls_connection_string, query);
        return result;

    def getPDMatchData(self, commandid):
        query = "SELECT command.CommandID,convert(varchar(19),MIN(command.CreateTime),120) AS StartTime,convert(varchar(19),lhs.End_Date,120) AS EndTime,SUM(rhs.Num_Rows) AS Num_Rows," + \
                "DateDiff(s, MIN(command.CreateTime), lhs.End_Date) AS Duration,lhs.Root_Operation_UID " + \
                " FROM [LEDataDB_30].[dbo].[MatcherService_MatcherQueue] lhs WITH(NOLOCK) " + \
                " INNER JOIN [GEDemo].[dbo].[OperationLog] rhs WITH(NOLOCK) ON lhs.Root_Operation_UID = rhs.Root_Operation_UID " + \
                " INNER JOIN [PropDataMatchDB].[dbo].[Commands] command WITH(NOLOCK) ON lhs.Root_Operation_UID = command.Process_UID " + \
                " INNER JOIN [PropDataMatchDB].[dbo].[MatcherClient_SystemStatus] systemStatus WITH (NOLOCK) on command.Process_UID = systemStatus.Root_Operation_UID " + \
                " WHERE command.CommandID=%s and DateDiff(hour, lhs.Create_Date, GETUTCDATE()) <= 520 " % (commandid) + \
                " GROUP BY lhs.Contract_External_ID, lhs.Create_Date,lhs.End_Date,lhs.Last_Execution_Date,lhs.Processing_State,lhs.Root_Operation_UID, " + \
                " command.CommandID,systemStatus.Root_Operation_UID,systemStatus.AvgBlockProcessingTime,lhs.Target_Tables " + \
                " ORDER BY lhs.Create_Date DESC, DateDiff(minute, lhs.Create_Date, lhs.End_Date) DESC ";

        #         print query;
        result = self.getQuery(pd_connection_string, query);
        return result;

    def getLeadScoringData(self, commandid):
        query = "SELECT [CommandId],convert(varchar(19),[BeginTime],120) as StartTime,convert(varchar(19),[EndTime],120) as EndTime,datediff(s,[BeginTime],[EndTime]) as duration " + \
                " FROM [LeadScoringDB].[dbo].[LeadScoringResult] where commandid=%s " % (commandid)
        #         print query;
        result = self.getQuery(ld_connection_string, query);
        return result;

    def getBardInData(self, queryName, launchid):
        query = "SELECT top 1 [LeadInputQueue_ID],[LEDeployment_ID],[Table_Name],[Total],[Lower],[Status],convert(varchar(19),[Populated],120) as StartTime,convert(varchar(19),[Consumed],120) as EndTime " + \
                " ,datediff(s,[Populated],[Consumed]) as duration " + \
                " FROM [LeadInputQueue] where table_name like '%s_%s_%s_%%'" % (self.tenantName, queryName, launchid) + \
                " order by LeadInputQueue_ID desc "
        #         print query
        result = self.getQuery(pls_connection_string, query);
        return result;

    def getPDMatchData_dataloader(self, launchid):
        query = "SELECT [CommandId],convert(varchar(19),[CreateTime],120) as job_createtime,convert(varchar(19),[StartTime],120) as StartTime,convert(varchar(19),[EndTime],120) as EndTime,datediff(s,[StartTime],[EndTime]) as [dl_duration_time] " + \
                " FROM [DataLoader].[dbo].[LaunchPDMatches] where [LaunchId]=%s" % (launchid)
        result = self.getQuery(dl_connection_string, query);
        return result;

    def getLeadScoringData_dataloader(self, launchid):
        query = "SELECT [LeadScoringId],[LaunchId],[LeadScoringCommandId] as commandid,[LeadInputTable] " + \
                ",convert(varchar(19),[CreateTime],120) as job_createtime,convert(varchar(19),[StartTime],120) as StartTime,convert(varchar(19),[EndTime],120) as EndTime " + \
                ",datediff(s,[StartTime],[EndTime]) as dl_duration_time " + \
                " FROM [DataLoader].[dbo].[LaunchLeadScorings] where launchid=%s" % (launchid)
        #         print query;
        result = self.getQuery(dl_connection_string, query);
        return result;

    def getBardInData_dataloader(self, launchid):
        query = "SELECT [BardId],[LaunchId],[QueryName],[CreateTime],convert(varchar(19), case when [StartTime] is null then [CreateTime] else starttime end, 120) as starttime " + \
                ",convert(varchar(19),[EndTime],120) as EndTime,datediff(s,case when [StartTime] is null then [CreateTime] else starttime end,[EndTime]) as dl_duration_time " + \
                " FROM [DataLoader].[dbo].[LaunchLSSBardIns] where launchid = %s" % (launchid)
        result = self.getQuery(dl_connection_string, query);
        return result;

    def getDanteData_dataloader(self, launchid):
        query = "select convert(varchar(19),min(starttime),120) as starttime,convert(varchar(19),max(endtime),120) as endtime, datediff(s,min(starttime),max(endtime)) as dl_duration_time " + \
                " FROM [DataLoader].[dbo].[LaunchQueries] where launchid =%s" % (launchid)
        result = self.getQuery(dl_connection_string, query);
        return result;

    def getLaunchData(self, groupName):
        query = "SELECT top 1 l.[LaunchId],l.[TenantId],convert(varchar(19),l.[CreateTime],120) as group_createtime,convert(varchar(19),l.[ValidateStart],120) as ValidateStart,convert(varchar(19),l.[ValidateEnd],120) as ValidateEnd,convert(varchar(19),l.[GenerateBCPStart],120) as GenerateBCPStart,convert(varchar(19),l.[GenerateBCPEnd],120) as GenerateBCPEnd " + \
                ",convert(varchar(19),l.[LaunchStart],120) as LaunchStart,convert(varchar(19),l.[LaunchEnd],120) as LaunchEnd,datediff(s,l.[ValidateStart],l.[LaunchEnd]) as [dl_launch_duration_time]" + \
                " FROM [DataLoader].[dbo].[Launches] l,[DataLoader].[dbo].[tenant] t " + \
                " where l.tenantid=t.tenantid and t.name='%s' and l.groupname='%s' " % (self.tenantName, groupName) + \
                "order by launchid desc";
        #         print query;
        result = self.getQuery(dl_connection_string, query);
        #         print result;
        if 0 == len(result):
            print "========>there is no data be found for conn: %s" % dl_connection_string;
            print ""
            print "========>the query is: %s" % query;
            return -1;
        else:
            return result[0];

    def performanceDataRecord(self, sequence, testingName, loadgroupName, serviceName, recordNumber,
                              dl_launch_begin_date=None, dl_launch_end_date=None, dl_launch_duration_time=0,
                              dl_begin_date=None, dl_end_date=None, dl_duration_time=0,
                              begin_date=None, end_date=None, duration_time=0, actuall_number=-1,
                              QueryName=None, job_name=None, pls_version=PLSEnvironments.pls_version):
        query = "INSERT INTO [BasicDataForIntegrationTest].[dbo].[performance_time_cost]" + \
                "([sequence],[maps],[testing_name],[LoadGroup_name],[service_name],[record_number],[dl_launch_begin_date],[dl_launch_end_date],[dl_launch_duration_time]" + \
                ",[dl_begin_date],[dl_end_date],[dl_duration_time],[begin_date],[end_date],[duration_time],[QueryName],[job_name],[pls_version],[tenant_name],[actuall_number]) " + \
                "VALUES(%d,'%s','%s','%s','%s',%d,'%s','%s',%d,'%s','%s',%d,'%s','%s',%d,'%s','%s','%s','%s',%d) " % (
                    sequence, self.app, testingName, loadgroupName, serviceName, recordNumber,
                    dl_launch_begin_date or '1900-01-01', dl_launch_end_date or '1900-01-01',
                    dl_launch_duration_time or 0,
                    dl_begin_date or '1900-01-01', dl_end_date or '1900-01-01', dl_duration_time or 0,
                    begin_date or '1900-01-01', end_date or '1900-01-01', duration_time or 0, QueryName, job_name,
                    pls_version, self.tenantName, actuall_number)
        #         print query
        return self.execQuery(test_connection_string, query);

    def prepareModelBulkData(self, model_number, from_date):
        # Wait for the leads to be scored
        dlc = SessionRunner()
        connection_string = PLSEnvironments.SQL_BasicDataForIntegrationTest;

        if (self.app == PLSEnvironments.pls_marketing_app_ELQ):
            query_map = "exec [PLS_ELQ_ELQ_Performance].[dbo].[GEN_Model_Bulk_DataSet] '%s', '%s'" % (
                model_number, from_date);
            query_crm = "exec [PLS_ELQ_SFDC_Performance].[dbo].[GEN_Model_Bulk_DataSet] '%s', '%s'" % (
                model_number, from_date);
        elif (self.app == PLSEnvironments.pls_marketing_app_MKTO):
            query_map = "exec [PLS_MKTO_MKTO_Performance].[dbo].[GEN_Model_Bulk_DataSet] '%s', '%s'" % (
                model_number, from_date);
            query_crm = "exec [PLS_MKTO_SFDC_Performance].[dbo].[GEN_Model_Bulk_DataSet] '%s', '%s'" % (
                model_number, from_date);
        # print self.connection_string;
        #         print query;
        print "GEN_Model_Bulk_DataSet"
        print dlc.execBulkProc(connection_string, query_map);
        print dlc.execBulkProc(connection_string, query_crm);

    def prepareHourlyData(self, model_number, loop):
        # Wait for the leads to be scored
        dlc = SessionRunner()
        connection_string = PLSEnvironments.SQL_BasicDataForIntegrationTest;

        if (self.app == PLSEnvironments.pls_marketing_app_ELQ):
            query_map = "exec [PLS_ELQ_ELQ_Performance].[dbo].[GEN_HourlyScoring] '%s', '%s'" % (model_number, loop);
            query_crm = "exec [PLS_ELQ_SFDC_Performance].[dbo].[GEN_HourlyScoring] '%s', '%s'" % (model_number, loop);
        elif (self.app == PLSEnvironments.pls_marketing_app_MKTO):
            query_map = "exec [PLS_MKTO_MKTO_Performance].[dbo].[GEN_HourlyScoring] '%s', '%s'" % (model_number, loop);
            query_crm = "exec [PLS_MKTO_SFDC_Performance].[dbo].[GEN_HourlyScoring] '%s', '%s'" % (model_number, loop);
        # print self.connection_string;
        #         print query;
        print "GEN_HourlyScoring"
        print dlc.execBulkProc(connection_string, query_map);
        print dlc.execBulkProc(connection_string, query_crm);


class PerformanceTest(SessionRunner):
    def __init__(self, marketting_app, tenant_name=None, bard_admin=None, host=PLSEnvironments.pls_test_server,
                 logfile=None, exception=False):
        super(PerformanceTest, self).__init__(host, logfile)
        self.exception = exception
        self.app = marketting_app;
        self.tenantName = tenant_name
        self.bardAdmin = bard_admin

    def PerformanceModelingTest(self, testName, data_number):
        pd = PerformanceData(tenant_name=self.tenantName, marketting_app=self.app);
        pd.prepareModelBulkData(data_number, '1900-01-01')

        editPerformanceRefreshDataSources(self.tenantName, self.app);

        PlsOperations.runModelingLoadGroups(self.tenantName, self.app, True);
        PlsOperations.updateModelingServiceSettings(self.bardAdmin);
        PlsOperations.activateModel(self.bardAdmin, self.tenantName);

        pd.recordPDMatchData(testName, data_number, "ModelBuild_PropDataMatch");
        pd.recordLeadScoringData(testName, data_number);

    def PerformanceBulkTest(self, testName, data_number):
        pd = PerformanceData(tenant_name=self.tenantName, marketting_app=self.app);
        pd.prepareModelBulkData(data_number, '1900-01-01')

        # Step 4 - Run LoadGroups and activate Model
        editPerformanceRefreshDataSources(self.tenantName, self.app);

        PlsOperations.runBulkScoring(self.tenantName, self.app);

        print "start collect the data ......"

        print "get PDMatch Data"
        pd.recordPDMatchData(testName, data_number, "ModelBuild_PropDataMatch");
        print "get BardIn Data"
        pd.recordBardInData(testName, data_number, "BulkScoring_PushToScoringDB");
        print "get contact dante data"
        pd.recordDanteData(testName, data_number, "PushDanteContactsAndAnalyticAttributesToDante")
        print "get lead dante data"
        pd.recordDanteData(testName, data_number, "PushDanteLeadsAndAnalyticAttributesToDante")

    def PerformanceHourlyTest(self, testName, data_number, loop):
        pd = PerformanceData(tenant_name=self.tenantName, marketting_app=self.app);
        pd.prepareHourlyData(data_number, loop)

        # Step 4 - Run LoadGroups and activate Model
        editHourlyPerformanceRefreshDataSources(self.tenantName, self.app);

        PlsOperations.runHourlyScoring(self.tenantName);
        PlsOperations.runHourlyDanteProcess(self.tenantName);

        print "start collect the data ......"

        print "get PDMatch Data"
        pd.recordPDMatchData(testName, data_number, "PropDataMatch");
        print "get BardIn Data"
        pd.recordBardInData(testName, data_number, "PushToScoringDB");
        print "get contact dante data"
        pd.recordDanteData(testName, data_number, "PushDanteContactsAndAnalyticAttributesToDante")
        print "get lead dante data"
        pd.recordDanteData(testName, data_number, "PushDanteLeadsAndAnalyticAttributesToDante")


class VisiDBRollBack(SessionRunner):
    def __init__(self, dl_server=PLSEnvironments.dl_server, logfile=None, exception=False):
        super(VisiDBRollBack, self).__init__(dl_server, logfile);
        self.exception = exception;
        self.dl_server = dl_server;
        #         self.dlUI = webdriver.Firefox();
        self.dlc_host = "http://%s:5000" % PLSEnvironments.visidb_server
        self.dlc_path = PLSEnvironments.dlc_path
        self.dl_user = PLSEnvironments.dl_server_user
        self.dl_pwd = PLSEnvironments.dl_server_pwd
        self.visidb_conn = "ServerName=%s;UserID=%s;Password=%s;" % (
            PLSEnvironments.visidb_server, PLSEnvironments.visidb_server_user, PLSEnvironments.visidb_server_pwd)

    def dlLogin(self, tenant_name):
        self.dlUI.get(self.dl_server);
        time.sleep(15);
        self.dlUI.find_element_by_id("text_email_login").clear();
        self.dlUI.find_element_by_id("text_email_login").send_keys(PLSEnvironments.dl_server_user);
        self.dlUI.find_element_by_id("text_password_login").clear();
        self.dlUI.find_element_by_id("text_password_login").send_keys(PLSEnvironments.dl_server_pwd);
        self.dlUI.find_element_by_xpath("//input[@value='Sign In']").click();
        time.sleep(30);

        self.dlUI.find_element_by_xpath("//li[@id='li_account']/span").click();
        self.dlUI.find_element_by_id("li_changetenant").click();

        time.sleep(30);
        tenants = self.dlUI.find_elements_by_xpath("//ul[@id='ul_account_changetenant']/li");

        count = 0;
        tenantExist = False;
        while count < len(tenants):
            tenantName = tenants[count].find_element_by_xpath("label/span").text;
            if tenant_name == tenantName[0:tenantName.find("(")].strip():
                tenants[count].find_element_by_xpath("label/input").click();
                tenantExist = True;
                break;
            count += 1;
        if tenantExist:
            self.dlUI.find_element_by_xpath("//button[@type='button']").click();

        time.sleep(30);
        self.dlUI.find_element_by_id("div_mainmenu_visidb").click();
        time.sleep(60);

    def dlGetRevision(self, tenant_name):
        self.dlLogin(tenant_name);
        #         revisionid = self.dlUI.find_element_by_id("revisionid_visidbstudio").get_attribute("value");
        self.dlUI.find_element_by_xpath("//ul[@id='toolbar_visidbstudio']/li[3]").click();
        rev_path = "//table[@id='table_revisions_visidbstudio']/tbody/tr[@class='']";
        time.sleep(30);
        revisionid = self.dlUI.find_element_by_xpath(rev_path).get_attribute("rid");
        self.dlUI.close();
        return revisionid

    def dlRollBack(self, tenant_name, revisionid):
        self.dlLogin(tenant_name);
        self.dlUI.find_element_by_xpath("//ul[@id='toolbar_visidbstudio']/li[3]").click();
        rev_path = "//table[@id='table_revisions_visidbstudio']/tbody/tr[@rid='%s']" % revisionid
        time.sleep(30);
        self.dlUI.find_element_by_xpath(rev_path).click();
        self.dlUI.find_element_by_xpath("//ul[@id='toolstrip_revisionmanagement_visidbstudio']/li[2]").click();

        self.dlUI.switch_to_alert().accept();
        self.dlUI.close();

    def detachVisidbDb(self, tenant_name):
        dlc = DLCRunner(host=self.dlc_host, dlc_path=self.dlc_path)
        params = {"-s": self.dl_server,
                  "-u": self.dl_user,
                  "-p": self.dl_pwd,
                  "-dc": self.visidb_conn,
                  "-dn": tenant_name
                  }
        command = "DeTach Visidb DataBase"

        print "Detaching visidb DB %s" % tenant_name
        status = dlc.runDLCcommand(command, params, local=False);
        return status

    def attachVisidbDb(self, tenant_name):
        dlc = DLCRunner(host=self.dlc_host, dlc_path=self.dlc_path)
        dfd = "%s\%s\%s.vdb" % (PLSEnvironments.visidb_data_folder, tenant_name, tenant_name)
        params = {"-s": self.dl_server,
                  "-u": self.dl_user,
                  "-p": self.dl_pwd,
                  "-dc": self.visidb_conn,
                  "-dn": tenant_name,
                  "-cl": "4096",
                  "-dfd": dfd
                  }
        command = "Attach Visidb DataBase"

        print "Attach visidb DB %s" % tenant_name
        status = dlc.runDLCcommand(command, params, local=False)

        return status;

    def bakVisidbDB(self, tenant_name):
        if self.isExistVisidbDB(tenant_name):
            return True;
        else:
            dlc = DLCRunner(host=self.dlc_host, dlc_path=self.dlc_path)
            command = "xcopy %s\%s %s\%s /E /I /Y" % (
                PLSEnvironments.visidb_data_folder, tenant_name, PLSEnvironments.visidb_data_bak, tenant_name)
            print "copying the db to bak folders"
            status = dlc.runCommand(command);

            return status;

    def copyBackVisidbDB(self, tenant_name):
        self.detachVisidbDb(tenant_name);
        dlc = DLCRunner(host=self.dlc_host, dlc_path=self.dlc_path)
        command = "xcopy %s\%s %s\%s /E /I /Y" % (
            PLSEnvironments.visidb_data_bak, tenant_name, PLSEnvironments.visidb_data_folder, tenant_name)
        print "copying the db back to data folder"
        dlc.runCommand(command);

        return self.attachVisidbDb(tenant_name);

    def isExistVisidbDB(self, tenant_name):
        dlc = DLCRunner(host=self.dlc_host, dlc_path=self.dlc_path)
        command = "mkdir %s\%s" % (PLSEnvironments.visidb_data_bak, tenant_name)

        dlc.runCommand(command);
        errorMessage = "A subdirectory or file %s\\%s already exists" % (PLSEnvironments.visidb_data_bak, tenant_name)
        if -1 == dlc.request_text[0].find(errorMessage):
            return False;
        else:
            return True;
