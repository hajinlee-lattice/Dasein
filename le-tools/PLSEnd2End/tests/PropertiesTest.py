'''
Created on Mar 18, 2015

@author: smeng
'''
import unittest
import requests
import json
import datetime
from Properties import PLSEnvironments
from operations.TestHelpers import JamsRunner
from operations.PerformanceHelpers import PerformanceData
from operations.PerformanceHelpers import VisiDBRollBack
from operations.TestHelpers import LPConfigRunner
from operations.LeadCreator import SFDCRequest
from operations.LeadCreator import EloquaRequest
from operations.LeadCreator import MarketoRequest


class Test(unittest.TestCase):

    def testPLSEnvironmentsPrint(self):
        print "the first try on this: ";
        print PLSEnvironments.pls_server;
        print PLSEnvironments.pls_server_folder;
        print PLSEnvironments.pls_bard_1;
        print PLSEnvironments.pls_bard_2;
        print PLSEnvironments.pls_url_1;
        print PLSEnvironments.pls_url_2;
        print PLSEnvironments.pls_pretzel;
        print PLSEnvironments.pls_bardAdminTool_1;
        print PLSEnvironments.pls_bardAdminTool_2;
        print PLSEnvironments.pls_db_server;
        print PLSEnvironments.pls_db_ScoringDaemon;

    @unittest.skip("")
    def testJamsCFG(self):
        print "for jams configurations"
        jams = JamsRunner();
        print jams.setJamsTenant(PLSEnvironments.pls_bard_2);

    def testFail(self):
#         assert False, "failing on purpose, expecting to see this text..."
        sss = PLSEnvironments.pls_SFDC_url
        print sss[0:sss.find("services")]

    def testLPTenantConsoleLogin(self):
        lp = LPConfigRunner();
        resp = lp.tenantConsoleLogin();
        print resp;

    def testLPModelLogin(self):
        lp = LPConfigRunner();
        resp = lp.modelLogin();
        print resp;

    def testLPNewTenantElq(self):
        lp = LPConfigRunner();
        resp = lp.addNewTenant("TestElq_11_16", "Eloqua", "10.41.1.247", "2.0", "BODCDEVVINT187", None);
        print resp;

    def testLPNewTenantMKTO(self):
        lp = LPConfigRunner();
        resp = lp.addNewTenant("TestMKTO_10_26_1", "Marketo", "10.41.1.247", "2.0", "BODCDEVVINT187", None);
        print resp;

    def testLPNewTenantSFDC(self):
        lp = LPConfigRunner();
        resp = lp.addNewTenant("TestSFDC_10_26_1", "SFDC", "10.41.1.247", "2.0", "BODCDEVVINT187", None);
        print resp;

    def testlpSFDCCredentials(self):
        lp = LPConfigRunner();
        resp = lp.lpSFDCCredentials("TestSFDC_10_16");
        print resp;

    def testlpElQCredentials(self):
        lp = LPConfigRunner();
        resp = lp.lpElQCredentials("TestElq_10_16");
        print resp;

    def testlpMKTOCredentials(self):
        lp = LPConfigRunner();
        resp = lp.lpMKTOCredentials("TestMKTO_10_29_70657");
        print resp;

    def testlpActivateModel(self):
        lp = LPConfigRunner();
        print lp.lpActivateModel("TestSFDC_201_1112");

    def testAddAnonymousLeadsToElq(self):
        elq = EloquaRequest()
        resp = elq.addAnonymousContact()
        print resp
        
    def addEloquaContactInCanada(self):
        elq = EloquaRequest()
        resp = elq.addEloquaContactWithCountry("Canada")
        print resp
        
    def addEloquaContactInUS(self):
        elq = EloquaRequest()
        resp = elq.addEloquaContactWithCountry("United States")
        print resp

    def testaddLeadToMarketo(self):
        mkto = MarketoRequest();
        print mkto.addLeadToMarketo(3); 
    def testaddLeadToMarketoForDante(self):
        mkto = MarketoRequest();
        print mkto.addLeadToMarketoForDante(3);   
    def testAddContactsToSFDC(self):
        sfdc = SFDCRequest();
        print sfdc.addContactsToSFDC(3)
           
    def testAddLeadsToSFDC(self):
        sfdc = SFDCRequest();
        print sfdc.addLeadsToSFDC(3)
        
    def testGetLaunchData(self):
        pd = PerformanceData(marketting_app = PLSEnvironments.pls_marketing_app_ELQ ,tenant_name = "BD_ADEDTBDd70064747nC26263627n1");
        pd.getLaunchData("aa");

    def testGetPDMatchData(self):
        pd = PerformanceData(marketting_app = PLSEnvironments.pls_marketing_app_ELQ ,tenant_name = "BD_ADEDTBDd70064747nC26263627n1");
        pd.recordPDMatchData("aa",123);

    def testGetLeadScoringData(self):
        pd = PerformanceData(marketting_app = PLSEnvironments.pls_marketing_app_ELQ ,tenant_name = "BD_ADEDTBDd70064747nC26263627n1");
        pd.recordLeadScoringData("aa",123);

    def testGetBardInData(self):
        pd = PerformanceData(marketting_app = PLSEnvironments.pls_marketing_app_ELQ ,tenant_name = "BD_ADEDTBDd70064747nY26263627n1");
#         pd.recordBardInData("aa",123,"BulkScoring_PushToScoringDB");
        pd.recordBardInData("testGetBardInData",222,"PushToScoringDB");

    def testGetDanteData(self):
        pd = PerformanceData(marketting_app = PLSEnvironments.pls_marketing_app_ELQ ,tenant_name = "BD_ADEDTBDd70064747nC26263627n1");
#         pd.recordBardInData("aa",123,"BulkScoring_PushToScoringDB");
        pd.recordDanteData("testGetDanteData",222,"PushDanteContactsAndAnalyticAttributesToDantes");
    def testBasicTestPerformance(self):
        pd = PerformanceData(marketting_app = PLSEnvironments.pls_marketing_app_ELQ ,tenant_name = "BD_ADEDTBDd70064747nC26263627n1");
#         pd.recordBardInData("aa",123,"BulkScoring_PushToScoringDB");
#         pd.recordDanteData("testGetDanteData",222,"PushDanteContactsAndAnalyticAttributesToDante");
        print pd.prepareModelBulkData(10000,'1900-01-01')

    def testVisiDBLogin(self):
        visidb = VisiDBRollBack();
        
        visidb.dlLogin("BD2_ADEDTBDd710159nP27060n1542")
        
    def testVisiDBGetRevisionId(self):
        visidb = VisiDBRollBack();        
        print visidb.dlGetRevision("BD2_ADEDTBDd710159nP27060n1542")
    def testVisiDBRollBack(self):
        visidb = VisiDBRollBack();
        visidb.dlRollBack("BD2_ADEDTBDd710159nP27060n1542",109);
        
    def testDetachVisidb(self):
        visidb = VisiDBRollBack();
        print visidb.detachVisidbDb("BD_ADEDTBDd70064747nG26263627r1");
        
    def testAttachVisidb(self):
        visidb = VisiDBRollBack();
        print visidb.attachVisidbDb("BD_ADEDTBDd70064747nG26263627r1");
        
    def testBakVisidbDB(self):
        visidb = VisiDBRollBack();
        print visidb.bakVisidbDB("BD_ADEDTBDd70064747nH26263627r1");
        
    def testCopyBackVisidbDB(self):
        visidb = VisiDBRollBack();
        print visidb.copyBackVisidbDB("BD_ADEDTBDd70064747nG26263627r1");
        
    def testIsExistVisidbDB(self):
        visidb = VisiDBRollBack();
        print visidb.isExistVisidbDB("BD_ADEDTBDd70064747nG26263627r1");
        
    def testGetQueryMetaDataColumns(self):
        passed=0;
        failed=0;
        for i in range(300):
            print "==========> %d" % i
            beginTime = datetime.datetime.now()
            response = self.runGetQueryMetaDataClolumns();
            if response.status_code == 200:
                endTime = datetime.datetime.now()
                duration = (endTime-beginTime).seconds
                if duration<=30:
                    passed +=1
                else:
                    print duration
                    failed +=1
            else:
                print response.text;    
        print "there are %d cost less than 30 seconds, %d more than 30 seconds" % (passed,failed) 
        
    def testGetQueryMetaDataColumnsWithSSL(self):
        passed=0;
        failed=0;
        for i in range(2):
            print "==========> %d" % i
            beginTime = datetime.datetime.now()
            response = self.runGetQueryMetaDataClolumnsWithSSL();
            if response.status_code == 200:
                endTime = datetime.datetime.now()
                duration = (endTime-beginTime).seconds
                if duration<=30:
                    passed +=1
                else:
                    print duration
                    failed +=1
            else:
                print response.text;    
        print "there are %d cost less than 30 seconds, %d more than 30 seconds" % (passed,failed) 
           
    def runGetQueryMetaDataClolumns(self):
        url = "http://bodcdevvint187.dev.lattice.local:8081//DLRestService/GetQueryMetaDataColumns"
        headers = {"Content-Type":"application/json","Accept": "application/json", "MagicAuthentication":"Security through obscurity!"};
        
        parameters = {}
        parameters["tenantName"] = "BD2_ADEDTBDd70064747nN26263627r12";
        parameters["queryName"] = "Q_PLS_Modeling";
        
        response = requests.post(url,headers = headers,data=json.dumps(parameters));
        return response;
        
    def runGetQueryMetaDataClolumnsWithSSL(self):
        url = "https://bodcdevvint187.dev.lattice.local:8080//DLRestService/GetQueryMetaDataColumns"
        headers = {"Content-Type":"application/json","Accept": "application/json", "MagicAuthentication":"Security through obscurity!"};
        
        parameters = {}
        parameters["tenantName"] = "BD2_ADEDTBDd70064747nN26263627r12";
        parameters["queryName"] = "Q_PLS_Modeling";
        
        response = requests.post(url,headers = headers,data=json.dumps(parameters),verify=True);
        return response;
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()