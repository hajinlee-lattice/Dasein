'''
Created on Mar 12, 2015

@author: smeng
'''

from ConfigParser import SafeConfigParser
from argparse import ArgumentParser
import platform
import sys


def main():
    parser = ArgumentParser()
    parser.add_argument("-s", "--server", dest="dt_server",
                        help="dante server name")
    parser.add_argument("-d", "--database", dest="dt_database",
                        help="dante database")
    parser.add_argument("-u", "--user", dest="dt_user",
                        help="dante database user")
    parser.add_argument("-p", "--password", dest="dt_password",
                        help="dante database password")

    args = parser.parse_args()
    print str(args)
    if args.dt_server:
        print "Change dante server to %s" % args.dt_server
        PLSEnvironments.SetConfig('DanteInfo', 'dante_server_name', args.dt_server)
    if args.dt_database:
        print "Change dante database to %s" % args.dt_database
        PLSEnvironments.SetConfig('DanteInfo', 'dante_server_db', args.dt_database)
    if args.dt_user:
        print "Change dante db user to %s" % args.dt_user
        PLSEnvironments.SetConfig('DanteInfo', 'dante_server_user', args.dt_user)
    if args.dt_password:
        print "Change dante db password to %s" % args.dt_password
        PLSEnvironments.SetConfig('DanteInfo', 'dante_server_pwd', args.dt_password)


class PLSEnvironments(object):
    # parser that read configuration properties from config.ini file
    parser = SafeConfigParser()
    parser.read("config.ini")

    # properties definition
    pls_bard_1 = "AutoJekinsElq_01_10_1452474931"  # %s_%s" % (time.strftime('%m_%d'),int(time.time()));
    pls_bard_2 = "AutoJekinsMKTO_11_16_1447681859"  # %s_%s" % (time.strftime('%m_%d'),int(time.time()));
    pls_bard_3 = "AutoJekinsSFDC_12_04_1449243201"  # %s_%s" % (time.strftime('%m_%d'),int(time.time()));
    pls_tenant_elq = parser.get("TestSetup", "pls_tenant_elq");
    pls_tenant_mkto = parser.get("TestSetup", "pls_tenant_mkto");
    pls_tenant_sfdc = parser.get("TestSetup", "pls_tenant_sfdc");
    pls_tenant_elq_model_activated = parser.get("TestSetup", "pls_tenant_elq_model_activated");
    pls_tenant_mkto_model_activated = parser.get("TestSetup", "pls_tenant_mkto_model_activated");
    pls_tenant_sfdc_model_Activated = parser.get("TestSetup", "pls_tenant_sfdc_model_Activated");
    load_group_timeout = parser.get("TestSetup", "load_group_timeout")

    pls_version = parser.get("TestSetup", "pls_version");
    dante_server_name = parser.get("DanteInfo", "dante_server_name");
    dante_server_db = parser.get("DanteInfo", "dante_server_db");
    dante_server_user = parser.get("DanteInfo", "dante_server_user");
    dante_server_pwd = parser.get("DanteInfo", "dante_server_pwd");
    jams_server = "10.41.1.247"
    dl_server_name = parser.get("DataLoaderVisidb", "dl_server_name");
    dl_database_instance = parser.get("DataLoaderVisidb", "dl_database_instance");
    dl_database_user = parser.get("DataLoaderVisidb", "dl_database_user")
    dl_database_password = parser.get("DataLoaderVisidb", "dl_database_password")
    visidb_server = parser.get("DataLoaderVisidb", "visidb_server_name");  # BODCDEVVINT187 or BODCDEVVINT207
    pls_test_server = "http://%s.dev.lattice.local:5000" % visidb_server;

    pls_tenant_console_url = "http://bodcdevvjty20.dev.lattice.local:8080"
    pls_tenant_console_user = "testuser1"
    pls_tenant_console_pwd = "Lattice1"
    pls_model_url = "http://bodcdevhdpweb52.dev.lattice.local:8080"
    pls_model_user = "bnguyen@lattice-engines.com"
    pls_model_pwd = "tahoe"
    pls_model_pwd_encryptd = "3ff74a580f8b39f039822455e92c2ef25658229622f16dc0f9918222c0be4900"

    dl_server = "https://%s.dev.lattice.local:8080/" % dl_server_name;
    dl_server_user = "richard.liu@lattice-engines.com";
    dl_server_pwd = "1";
    dlc_path = "\\\\10.41.1.55\DevelopmentShare\TestAutomation\DLC"  # "\\\\10.61.0.210\DevQA\share\Software\DLTools"
    visidb_server_user = "admin";
    visidb_server_pwd = "visid@t@b@se";

    template_location = "\\\\%s.dev.lattice.local\PLSTemplate" % visidb_server  # "\\\\10.61.0.210\DevQA\share\PLSTemplate";
    visidb_data_folder = "D:\\VisiDBData";
    visidb_data_bak = "D:\\performanceTest\\dbbak";

    pls_SFDC_login_url = "https://login.salesforce.com/"
    pls_SFDC_user = "apeters-widgettech@lattice-engines.com";
    pls_SFDC_pwd = "Happy2010";
    pls_SFDC_key = "oIogZVEFGbL3n0qiAp6F66TC";

    pls_ELQ_user = "Matt.Sable";
    pls_ELQ_pwd = "Lattice2";
    pls_ELQ_company = "TechnologyPartnerLatticeEngines";
    pls_ELQ_url = "https://secure.p03.eloqua.com/API/REST/1.0";

    pls_MKTO_url = "https://na-sj02.marketo.com/soap/mktows/2_0"  #
    pls_MKTO_Username = "latticeenginessandbox1_9026948050BD016F376AE6"
    pls_MKTO_Password = "41802295835604145500BBDD0011770133777863CA58"
    pls_MKTO_Client_url = "https://976-KKC-431.mktorest.com";
    pls_MKTO_Client_id = "868c37ad-905c-4562-be86-c6b1f39293f4";
    pls_MKTO_client_secret = "vBt3ZnFAU4eCyrtzOzRZfvkRQPfdDrUi";

    pls_SFDC_url = "https://na6.salesforce.com/services/data/v29.0";
    pls_SFDC_OAuth2 = "https://login.salesforce.com/services/oauth2/token";
    pls_SFDC_Client_id = "3MVG9CVKiXR7Ri5qgebamGAhq9XokRd5cV.66vOrj5_D7MREvbcFjeyTNWioPLJfd2CbCcMu9eAHqvtvLHiMv";
    pls_SFDC_client_secret = "2693772183877125172";
    pls_SFDC_OAuth2_user = "apeters-widgettech%40lattice-engines.com";
    pls_SFDC_OAuth2_pwd = "Happy2010oIogZVEFGbL3n0qiAp6F66TC";

    pls_marketing_app_ELQ = "ELQ";
    pls_marketing_app_MKTO = "MKTO";
    pls_marketing_app_SFDC = "SFDC";

    driverName = "SQL Server"
    platformName = platform.system();
    if platformName == "Linux" or platformName == "Darwin":
        driverName = "FreeTDS";

    connStr = "DRIVER={%s};" % driverName;

    SQL_JAMS_CFG = connStr + "SERVER=%s;DATABASE=JAMSCFG;uid=dataloader_user;pwd=password" % jams_server;
    SQL_ScoringDaemon = connStr + "SERVER=10.41.1.207\SQL2012STD;DATABASE=ScoringDaemon_QA;uid=dataloader_prod;pwd=L@ttice2";
    SQL_BasicDataForIntegrationTest = connStr + "SERVER=10.41.1.187\SQL2008;DATABASE=BasicDataForIntegrationTest;uid=dataloader_user;pwd=password";
    SQL_conn_dataloader = connStr + "SERVER=%s;DATABASE=DataLoader;uid=%s;pwd=%s;" % (
        dl_database_instance, dl_database_user, dl_database_password);
    SQL_conn_pdMatch = connStr + "SERVER=BODCPRODVSQL130;DATABASE=PropDataMatchDB;uid=dataloader_prod;pwd=L@ttice2;";
    SQL_conn_leadscoring = connStr + "SERVER=%s\sql2008r2;DATABASE=DataLoader;uid=dataloader_user;pwd=password;" % "10.41.1.187";
    SQL_conn_dante = connStr + "SERVER=%s.dev.lattice.local\SQL2012STD;DATABASE=%s;uid=%s;pwd=%s;" % (
        dante_server_name, dante_server_db, dante_server_user, dante_server_pwd);
    SQL_conn_SFDC_End2EndTest_Data = connStr + "SERVER=%s\Sql2008;DATABASE=PLS_SFDC_End2EndTest_Data;uid=dataloader_user;pwd=password;" % "10.41.1.187";

    # dataloader providers
    SQL_PropDataForModeling = "Data Source=bodcprodvsql130;" + \
                              "Initial Catalog=PropDataMatchDB;" + \
                              "Persist Security Info=True;" + \
                              "User ID=dataloader_prod;" + \
                              "Password=L@ttice2;"

    SQL_PropDataMatch = "Data Source=bodcprodvsql130;" + \
                        "Initial Catalog=PropDataMatchDB;" + \
                        "Persist Security Info=True;" + \
                        "User ID=dataloader_prod;" + \
                        "Password=L@ttice2;"

    SQL_PropDataForScoring = "Data Source=bodcprodvsql130;" + \
                             "Initial Catalog=PropDataMatchDB;" + \
                             "Persist Security Info=True;" + \
                             "User ID=dataloader_prod;" + \
                             "Password=L@ttice2;"

    SQL_LeadScoring = "Data Source=10.41.1.187\sql2008r2;" + \
                      "Initial Catalog=LeadScoringDB;" + \
                      "Persist Security Info=True;" + \
                      "User ID=dataloader_user;" + \
                      "Password=password;"
    SQL_MultiTenant = "Data Source=10.41.1.250\QACLUSTER,62836;" + \
                      "Initial Catalog=PLS_MultiTenant;" + \
                      "Persist Security Info=True;" + \
                      "User ID=dataplatformdev;" + \
                      "Password=welcome;"
    SQL_DanteDB_DataProvider = "Data Source=%s.dev.lattice.local\SQL2012STD;" % dante_server_name + \
                               "Initial Catalog=%s;" % dante_server_db + \
                               "Persist Security Info=True;" + \
                               "User ID=%s;" % dante_server_user + \
                               "Password=%s;" % dante_server_pwd
    SQL_LSSBard = "Data Source=10.41.1.207\SQL2012STD;" + \
                  "Initial Catalog=ScoringDaemon_QA;" + \
                  "Persist Security Info=True;" + \
                  "User ID=dataloader_prod;" + \
                  "Password=L@ttice2;"
    mock_ELQ_ELQ_DataProvider = "Data Source=10.41.1.187\sql2008;" + \
                                "Initial Catalog=PLS_ELQ_ELQ_ReleaseQA_20150930;" + \
                                "Persist Security Info=True;" + \
                                "User ID=dataloader_user;" + \
                                "Password=password;"
    mock_ELQ_SFDC_DataProvider = "Data Source=10.41.1.187\sql2008;" + \
                                 "Initial Catalog=PLS_ELQ_SFDC_ReleaseQA_20150930;" + \
                                 "Persist Security Info=True;" + \
                                 "User ID=dataloader_user;" + \
                                 "Password=password;"
    mock_MKTO_MKTO_DataProvider = "Data Source=10.41.1.187\sql2008;" + \
                                  "Initial Catalog=PLS_MKTO_MKTO_ReleaseQA_20150630;" + \
                                  "Persist Security Info=True;" + \
                                  "User ID=dataloader_user;" + \
                                  "Password=password;"
    mock_MKTO_SFDC_DataProvider = "Data Source=10.41.1.187\sql2008;" + \
                                  "Initial Catalog=PLS_MKTO_SFDC_ReleaseQA_20150630;" + \
                                  "Persist Security Info=True;" + \
                                  "User ID=dataloader_user;" + \
                                  "Password=password;"
    mock_SFDC_SFDC_DataProvider = "Data Source=10.41.1.187\sql2008;" + \
                                  "Initial Catalog=PLS_SFDC_Dataset01_20151028;" + \
                                  "Persist Security Info=True;" + \
                                  "User ID=dataloader_user;" + \
                                  "Password=password;"

    performance_ELQ_ELQ_DataProvider = "Data Source=10.41.1.187\sql2008;" + \
                                       "Initial Catalog=PLS_ELQ_ELQ_Performance;" + \
                                       "Persist Security Info=True;" + \
                                       "User ID=dataloader_user;" + \
                                       "Password=password;"
    performance_ELQ_SFDC_DataProvider = "Data Source=10.41.1.187\sql2008;" + \
                                        "Initial Catalog=PLS_ELQ_SFDC_Performance;" + \
                                        "Persist Security Info=True;" + \
                                        "User ID=dataloader_user;" + \
                                        "Password=password;"
    performance_MKTO_MKTO_DataProvider = "Data Source=10.41.1.187\sql2008;" + \
                                         "Initial Catalog=PLS_MKTO_MKTO_Performance;" + \
                                         "Persist Security Info=True;" + \
                                         "User ID=dataloader_user;" + \
                                         "Password=password;"
    performance_MKTO_SFDC_DataProvider = "Data Source=10.41.1.187\sql2008;" + \
                                         "Initial Catalog=PLS_MKTO_SFDC_Performance;" + \
                                         "Persist Security Info=True;" + \
                                         "User ID=dataloader_user;" + \
                                         "Password=password;"

    perf_CheckInterval = parser.get('PerformanceTest', 'interval')
    perfTable_GroupRunData = parser.get('PerformanceTest', 'perfTable_GroupRunData')

    def __init__(self):
        '''
        Constructor
        '''

    @staticmethod
    def SetConfig(section, option, value=None):
        parser = SafeConfigParser()
        parser.read("config.ini")
        parser.set(section, option, value)
        fileHandle = open('config.ini', 'w')
        parser.write(fileHandle)


if __name__ == '__main__':
    main()
