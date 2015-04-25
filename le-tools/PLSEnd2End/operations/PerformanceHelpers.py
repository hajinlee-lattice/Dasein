#!/usr/local/bin/python
# coding: utf-8

# import modules
# import modules
import requests
import json
import time
import base64
import random
import string

from Properties import PLSEnvironments
from operations.TestRunner import SessionRunner
from operations.TestHelpers import DLCRunner

test_connection_string = PLSEnvironments.SQL_BasicDataForIntegrationTest;
dl_connection_string = PLSEnvironments.SQL_conn_dataloader;
pd_connection_string = PLSEnvironments.SQL_conn_pdMatch;
ld_connection_string = PLSEnvironments.SQL_conn_leadscoring;
pls_connection_string = PLSEnvironments.SQL_ScoringDaemon;


def createPerformanceDataProviders(tenant, marketting_app,host=PLSEnvironments.pls_test_server, dlc_path=PLSEnvironments.dl_dlc_path,
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
def editPerformanceRefreshDataSources(tenant, marketting_app,host=PLSEnvironments.pls_test_server, dlc_path=PLSEnvironments.dl_dlc_path,
                               dl_server=PLSEnvironments.dl_server,
                               user=PLSEnvironments.dl_server_user,
                               password=PLSEnvironments.dl_server_pwd):

        dlc = DLCRunner(host=host, dlc_path=dlc_path)
        command = "Edit Refresh Data Source"
        params = {"-s": dl_server,
                  "-u": user,
                  "-p": password,
                  "-t": tenant,
                  "-f": "@recordcount(2000000)"
                 }
    
        #LoadCRMDataForModeling
        rds_list = ["SFDC_User", "SFDC_Contact", "SFDC_Lead", "SFDC_Opportunity", "SFDC_OpportunityContactRole"]
        for rds in rds_list:
            params["-g"] = "LoadCRMDataForModeling"
            params["-rn"] = rds
            params["-cn"] = "performance_SFDC_DataProvider"
            print "Updating Refresh Data Source %s for performance_SFDC_DataProvider" % rds
            dlc.runDLCcommand(command, params)
            #dlc.getStatus()
    
        #LoadMAPDataForModeling
        if marketting_app == "ELQ":
            params["-g"] = "LoadMAPDataForModeling"
            params["-rn"] = "ELQ_Contact"
            params["-cn"] = "performance_ELQ_ELQ_DataProvider";
            print "Updating Refresh Data SourceELQ_Contact for performance_ELQ_ELQ_DataProvider"
            dlc.runDLCcommand(command, params)
            #print dlc.getStatus()
            
    
        #"LoadMAPDataForModeling_ActivityRecord_OtherThanNewLead": "ActivityRecord_OtherThanNewLead",
        elif marketting_app == "MKTO":
            params["-cn"] = "performance_MKTO_MKTO_DataProvider"
            rds_dict = {"LoadMAPDataForModeling_ActivityRecord_NewLead": "ActivityRecord_NewLead",
                        "LoadMAPDataForModeling_LeadRecord": "MKTO_LeadRecord"}
            for lg in rds_dict:
                params["-g"] = lg
                params["-rn"] = rds_dict[lg]
                print "Updating Refresh Data Source %s for performance_Marketo_Data_Provider" % rds_dict[lg]
                dlc.runDLCcommand(command, params)
                #dlc.getStatus()
        else:
            print "!!![%s] MARKETTING UP IS NOT SUPPORTED!!!" % marketting_app



def getSequence(): 
        dlc = SessionRunner()  
        query = "select max(sequence) from performance_time_cost";      
        result = dlc.getQuery(test_connection_string, query);
        if None == result[0][0]:
            return 0;
        else:
            return result[0][0]+1;
        
class PerformanceData(SessionRunner):

    def __init__(self,marketting_app,tenant_name=None, host=PLSEnvironments.pls_test_server, logfile=None, exception=False):
        super(PerformanceData, self).__init__(host, logfile)
        self.exception = exception
        self.app = marketting_app;
        self.tenantName=tenant_name;
        self.sequence = getSequence();
    def recordDanteData(self,test_Name,record_number,groupName):
        dllaunchData = self.getLaunchData(groupName);
        dlDTData =self.getDanteData_dataloader(dllaunchData.get("launchid"));
        dtData = self.getDanteData(dlDTData.get("starttime"));
                 
        print self.performanceDataRecord(sequence=self.sequence,testingName=test_Name,loadgroupName=groupName,
                                   serviceName=pd_connection_string,recordNumber=record_number,
                                   dl_launch_begin_date=dllaunchData.get("validatestart"),dl_launch_end_date=dllaunchData.get("launchend"),dl_launch_duration_time=dllaunchData.get("dl_launch_duration_time"),
                                   dl_begin_date=dlDTData.get("starttime"),dl_end_date=dlDTData.get("endtime"),dl_duration_time=dlDTData.get("dl_duration_time"),
                                   begin_date=dtData.get("starttime"),end_date=dtData.get("endtime"),duration_time=dtData.get("duration"),actuall_number=dtData.get("num_rows"))
    def recordBardInData(self,test_Name,record_number,groupName):
        dllaunchData = self.getLaunchData(groupName);
        dlBIData =self.getBardInData_dataloader(dllaunchData.get("launchid"));
        biData = self.getBardInData(dlBIData.get("queryname"),dllaunchData.get("launchid"));
                
        print self.performanceDataRecord(sequence=self.sequence,testingName=test_Name,loadgroupName=groupName,
                                   serviceName=pd_connection_string,recordNumber=record_number,
                                   dl_launch_begin_date=dllaunchData.get("validatestart"),dl_launch_end_date=dllaunchData.get("launchend"),dl_launch_duration_time=dllaunchData.get("dl_launch_duration_time"),
                                   dl_begin_date=dlBIData.get("starttime"),dl_end_date=dlBIData.get("endtime"),dl_duration_time=dlBIData.get("dl_duration_time"),
                                   begin_date=biData.get("starttime"),end_date=biData.get("endtime"),duration_time=biData.get("duration"),QueryName=dlBIData.get("queryname"),actuall_number=biData.get("total"))
    def recordLeadScoringData(self,test_Name,record_number):
        dllaunchData = self.getLaunchData("CreateAnalyticPlay");
        dlLDData =self.getLeadScoringData_dataloader(dllaunchData.get("launchid"));
        ldData = self.getLeadScoringData(dlLDData.get("commandid"));
                
        print self.performanceDataRecord(sequence=self.sequence,testingName=test_Name,loadgroupName="CreateAnalyticPlay",
                                   serviceName=pd_connection_string,recordNumber=record_number,
                                   dl_launch_begin_date=dllaunchData.get("validatestart"),dl_launch_end_date=dllaunchData.get("launchend"),dl_launch_duration_time=dllaunchData.get("dl_launch_duration_time"),
                                   dl_begin_date=dlLDData.get("starttime"),dl_end_date=dlLDData.get("endtime"),dl_duration_time=dlLDData.get("dl_duration_time"),
                                   begin_date=ldData.get("starttime"),end_date=ldData.get("endtime"),duration_time=ldData.get("duration"))    
    def recordPDMatchData(self,test_Name,record_number):
        dllaunchData = self.getLaunchData("PropDataMatch");
        dlPDData =self.getPDMatchData_dataloader(dllaunchData.get("launchid"));
        pdData = self.getPDMatchData(dlPDData[0]);
                
        print self.performanceDataRecord(sequence=self.sequence,testingName=test_Name,loadgroupName="PropDataMatch",
                                   serviceName=pd_connection_string,recordNumber=record_number,
                                   dl_launch_begin_date=dllaunchData.get("validatestart"),dl_launch_end_date=dllaunchData.get("launchend"),dl_launch_duration_time=dllaunchData.get("dl_launch_duration_time"),
                                   dl_begin_date=dlPDData.get("starttime"),dl_end_date=dlPDData.get("endtime"),dl_duration_time=dlPDData.get("dl_duration_time"),
                                   begin_date=pdData.get("starttime"),end_date=pdData.get("endtime"),duration_time=pdData.get("duration"),actuall_number=pdData.get("num_rows"))
    
    def getDanteData(self,starttime):
        query = "select count(last_modification_date) as num_rows,convert(varchar(19),min(last_modification_date),120) as starttime, convert(varchar(19),max(last_modification_date),120) as endtime, datediff(s,min(last_modification_date),max(last_modification_date)) as duration " + \
                " from (select last_modification_date from [%s].[dbo].[LeadCache] where datediff(s,last_modification_date,'%s')<0) t" % (PLSEnvironments.pls_db_Dante,starttime)
#         print query
        result = self.getQuery(pls_connection_string, query);
        return result[0];
    def getPDMatchData(self,commandid):
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
#         query = "select @@servername"
        result = self.getQuery(pd_connection_string, query);
        return result[0];
    def getLeadScoringData(self,commandid):
        query = "SELECT [CommandId],convert(varchar(19),[BeginTime],120) as StartTime,convert(varchar(19),[EndTime],120) as EndTime,datediff(s,[BeginTime],[EndTime]) as duration " + \
                " FROM [LeadScoringDB].[dbo].[LeadScoringResult] where commandid=%s " % (commandid)
#         print query;
        result = self.getQuery(ld_connection_string, query);
        return result[0];
    def getBardInData(self,queryName,launchid):
        query = "SELECT top 1 [LeadInputQueue_ID],[LEDeployment_ID],[Table_Name],[Total],[Lower],[Status],convert(varchar(19),[Populated],120) as StartTime,convert(varchar(19),[Consumed],120) as EndTime " + \
                " ,datediff(s,[Populated],[Consumed]) as duration " + \
                " FROM [LeadInputQueue] where table_name like '%s_%s_%s_%%'" %(self.tenantName,queryName,launchid) + \
                " order by LeadInputQueue_ID desc "
#         print query
        result = self.getQuery(pls_connection_string, query);
        return result[0];
        
    def getPDMatchData_dataloader(self,launchid):
        query = "SELECT [CommandId],convert(varchar(19),[CreateTime],120) as job_createtime,convert(varchar(19),[StartTime],120) as StartTime,convert(varchar(19),[EndTime],120) as EndTime,datediff(s,[CreateTime],[EndTime]) as [dl_duration_time] " + \
              " FROM [DataLoader].[dbo].[LaunchPDMatches] where [LaunchId]=%s" % (launchid)
        result = self.getQuery(dl_connection_string, query);
        return result[0];
    def getLeadScoringData_dataloader(self,launchid):
        query = "SELECT [LeadScoringId],[LaunchId],[LeadScoringCommandId] as commandid,[LeadInputTable] " + \
                ",convert(varchar(19),[CreateTime],120) as job_createtime,convert(varchar(19),[StartTime],120) as StartTime,convert(varchar(19),[EndTime],120) as EndTime " + \
                ",datediff(s,[StartTime],[EndTime]) as dl_duration_time " + \
                " FROM [DataLoader].[dbo].[LaunchLeadScorings] where launchid=%s" % (launchid)
#         print query;
        result = self.getQuery(dl_connection_string, query);
        return result[0];
    def getBardInData_dataloader(self,launchid):
        query = "SELECT [BardId],[LaunchId],[QueryName],[CreateTime],convert(varchar(19), case when [StartTime] is null then [CreateTime] else starttime end, 120) as starttime " + \
                ",convert(varchar(19),[EndTime],120) as EndTime,datediff(s,case when [StartTime] is null then [CreateTime] else starttime end,[EndTime]) as dl_duration_time " + \
                " FROM [DataLoader].[dbo].[LaunchLSSBardIns] where launchid = %s" % (launchid)
        result = self.getQuery(dl_connection_string, query);
        return result[0];
    def getDanteData_dataloader(self,launchid):
        query = "select convert(varchar(19),min(starttime),120) as starttime,convert(varchar(19),max(endtime),120) as endtime, datediff(s,min(starttime),max(endtime)) as dl_duration_time " + \
                " FROM [DataLoader].[dbo].[LaunchQueries] where launchid =%s" % (launchid)
        result = self.getQuery(dl_connection_string, query);
        return result[0];
        
    def getLaunchData(self,groupName):
        query = "SELECT top 1 l.[LaunchId],l.[TenantId],convert(varchar(19),l.[CreateTime],120) as group_createtime,convert(varchar(19),l.[ValidateStart],120) as ValidateStart,convert(varchar(19),l.[ValidateEnd],120) as ValidateEnd,convert(varchar(19),l.[GenerateBCPStart],120) as GenerateBCPStart,convert(varchar(19),l.[GenerateBCPEnd],120) as GenerateBCPEnd " + \
            ",convert(varchar(19),l.[LaunchStart],120) as LaunchStart,convert(varchar(19),l.[LaunchEnd],120) as LaunchEnd,datediff(s,l.[ValidateStart],l.[LaunchEnd]) as [dl_launch_duration_time]" + \
            " FROM [DataLoader].[dbo].[Launches] l,[DataLoader].[dbo].[tenant] t " + \
            " where l.tenantid=t.tenantid and t.name='%s' and l.groupname='%s' "  % (self.tenantName,groupName) + \
            "order by launchid desc";
#         print query;
        result = self.getQuery(dl_connection_string, query);
        return result[0];
        
    def performanceDataRecord(self,sequence,testingName, loadgroupName,serviceName,recordNumber,
                              dl_launch_begin_date=None,dl_launch_end_date=None,dl_launch_duration_time=0,
                              dl_begin_date=None,dl_end_date=None,dl_duration_time=0,
                              begin_date=None,end_date=None,duration_time=0,actuall_number=0,
                              QueryName=None,job_name=None,pls_version=PLSEnvironments.pls_version):
        query = "INSERT INTO [BasicDataForIntegrationTest].[dbo].[performance_time_cost]" + \
               "([sequence],[maps],[testing_name],[LoadGroup_name],[service_name],[record_number],[dl_launch_begin_date],[dl_launch_end_date],[dl_launch_duration_time]" + \
               ",[dl_begin_date],[dl_end_date],[dl_duration_time],[begin_date],[end_date],[duration_time],[QueryName],[job_name],[pls_version],[tenant_name],[actuall_number]) " + \
               "VALUES(%d,'%s','%s','%s','%s',%d,'%s','%s',%d,'%s','%s',%d,'%s','%s',%d,'%s','%s','%s','%s',%d) " % (sequence,self.app,testingName,loadgroupName,serviceName,recordNumber,dl_launch_begin_date or '1900-01-01',dl_launch_end_date or '1900-01-01',dl_launch_duration_time or 0,dl_begin_date or '1900-01-01',dl_end_date or '1900-01-01',dl_duration_time or 0,begin_date or '1900-01-01',end_date or '1900-01-01',duration_time or 0,QueryName,job_name,pls_version,self.tenantName,actuall_number)
        print query
        return self.execQuery(test_connection_string, query);
    
    def prepareModelBulkData(self,model_number,from_date):
        # Wait for the leads to be scored
        dlc = SessionRunner()
        connection_string = PLSEnvironments.SQL_BasicDataForIntegrationTest; 
            
        if(self.app == PLSEnvironments.pls_marketing_app_ELQ):
            query_map="exec [PLS_ELQ_ELQ_Performance].[dbo].[GEN_Model_Bulk_DataSet] '%s', '%s'" % (model_number,from_date);
            query_crm="exec [PLS_ELQ_SFDC_Performance].[dbo].[GEN_Model_Bulk_DataSet] '%s', '%s'" % (model_number,from_date);
        elif(self.app == PLSEnvironments.pls_marketing_app_MKTO):
            query_map="exec [PLS_MKTO_MKTO_Performance].[dbo].[GEN_Model_Bulk_DataSet] '%s', '%s'" % (model_number,from_date);
            query_crm="exec [PLS_MKTO_SFDC_Performance].[dbo].[GEN_Model_Bulk_DataSet] '%s', '%s'" % (model_number,from_date);
#         print self.connection_string;
#         print query;
        print "GEN_Model_Bulk_DataSet"
        print dlc.execBulkProc(connection_string, query_map);
        print dlc.execBulkProc(connection_string, query_crm);
        
