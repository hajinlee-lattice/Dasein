'''
Created on Mar 18, 2015

@author: smeng
'''
import unittest
from Properties import PLSEnvironments
from operations.TestHelpers import JamsRunner
from operations.PerformanceHelpers import PerformanceData


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
        assert False, "failing on purpose, expecting to see this text..."


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
        pd.recordDanteData("testGetDanteData",222,"PushDanteContactsAndAnalyticAttributesToDante");
    def testBasicTestPerformance(self):
        pd = PerformanceData(marketting_app = PLSEnvironments.pls_marketing_app_ELQ ,tenant_name = "BD_ADEDTBDd70064747nC26263627n1");
#         pd.recordBardInData("aa",123,"BulkScoring_PushToScoringDB");
#         pd.recordDanteData("testGetDanteData",222,"PushDanteContactsAndAnalyticAttributesToDante");
        print pd.prepareModelBulkData(10000,'1900-01-01')

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()