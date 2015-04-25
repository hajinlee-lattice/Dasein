'''
Created on Mar 12, 2015

@author: smeng
'''

from ConfigParser import SafeConfigParser

class PLSEnvironments(object):
    
    # parser that read configuration properties from config.ini file
    parser = SafeConfigParser()
    parser.read('config.ini')
    
    
    #properties definition
    pls_server=parser.get('BuildInfo', 'pls_server');    
    pls_server_folder=parser.get('BuildInfo', 'install_dir');
    pls_bard_1=parser.get('BuildInfo', 'tenant');
    pls_bard_2=parser.get('BuildInfo', 'tenant_2');
    svn_location_local=parser.get('TestSetup', 'svn_location');
    pls_version=parser.get('TestSetup', 'pls_version');

    
    dl_server="https://bodcdevvint207.dev.lattice.local:8080/";
    visidb_server="bodcdevvint207.dev.lattice.local";
    
    
    pls_url_1="https://%s/%s" % (pls_server, pls_bard_1);
    pls_url_2="https://%s/%s" % (pls_server, pls_bard_2);
    pls_server_user="admin";
    pls_server_pwd="admin";
    
    pls_pretzel="%s\\%s\\Pretzel\\Install\\bin\\PretzelAdminTool.exe " % (pls_server_folder, pls_bard_1[3:]);
    pls_bardAdminTool_1="%s\\%s\\Bard\\Install\\bin\\BardAdminTool.exe " % (pls_server_folder, pls_bard_1[3:]);
    pls_bardAdminTool_2="%s\\%s\\Bard2\\Install\\bin\\BardAdminTool.exe " % (pls_server_folder, pls_bard_2[4:]);
    
    pls_test_server = "http://%s:5000" % pls_server;
    pls_install_dir = "%s\\%s" % (pls_server_folder, pls_bard_1[3:]);
    pls_db_server = "%s\\SQL2012STD" % pls_server;
    pls_db_ScoringDaemon= "SD_%s" % pls_bard_1[3:];
    pls_db_Dante= "DT_%s" % pls_bard_1[3:];
    
    dl_server_user="richard.liu@lattice-engines.com";
    dl_server_pwd="1";
    dl_dlc_path="%s\\%s\\ScoringDaemon\\ScoringDaemon\\bin\\Services\\DataLoaderShim" % (pls_server_folder, pls_bard_1[3:]);
    
    visidb_server_user="admin";
    visidb_server_pwd="visid@t@b@se";
    
    template_location="\\\\%s\PLSTemplate" % visidb_server#"\\\\10.61.0.210\DevQA\share\PLSTemplate";
    
    pls_SFDC_user="apeters-widgettech@lattice-engines.com";
    pls_SFDC_pwd="Happy2010";
    pls_SFDC_key="oIogZVEFGbL3n0qiAp6F66TC";
    
    pls_ELQ_user="Matt.Sable";
    pls_ELQ_pwd="Lattice1";
    pls_ELQ_company="TechnologyPartnerLatticeEngines";
    pls_ELQ_url="https://secure.p03.eloqua.com/API/REST/1.0";
    
    pls_MKTO_url="https://976-KKC-431.mktorest.com";
    pls_MKTO_Client_id = "868c37ad-905c-4562-be86-c6b1f39293f4";
    pls_MKTO_client_secret = "vBt3ZnFAU4eCyrtzOzRZfvkRQPfdDrUi";
    
    pls_SFDC_url="https://na6.salesforce.com/services/data/v29.0";
    pls_SFDC_OAuth2 = "https://login.salesforce.com/services/oauth2/token";
    pls_SFDC_Client_id = "3MVG9CVKiXR7Ri5qgebamGAhq9XokRd5cV.66vOrj5_D7MREvbcFjeyTNWioPLJfd2CbCcMu9eAHqvtvLHiMv";
    pls_SFDC_client_secret = "2693772183877125172";
    pls_SFDC_OAuth2_user = "apeters-widgettech%40lattice-engines.com";
    pls_SFDC_OAuth2_pwd = "Happy2010oIogZVEFGbL3n0qiAp6F66TC";
    
    pls_marketing_app_ELQ="ELQ";
    pls_marketing_app_MKTO="MKTO";
    pls_marketing_app_SFDC="SFDC";
    
    SQL_JAMS_CFG = "DRIVER={SQL Server};SERVER=10.41.1.247;DATABASE=JAMSCFG;uid=dataloader_user;pwd=password";
    SQL_ScoringDaemon = "DRIVER={SQL Server};SERVER=%s\SQL2012STD;DATABASE=SD_%s;uid=dataloader_prod;pwd=L@ttice2" % (pls_server, pls_bard_1[3:]);
    SQL_BasicDataForIntegrationTest = "DRIVER={SQL Server};SERVER=10.41.1.187\SQL2008;DATABASE=BasicDataForIntegrationTest;uid=dataloader_user;pwd=password";
    SQL_conn_dataloader = "DRIVER={SQL Server};SERVER=10.41.1.187\sql2008r2;DATABASE=DataLoader;uid=dataloader_user;pwd=password;";
    SQL_conn_pdMatch = "DRIVER={SQL Server};SERVER=BODCPRODVSQL130;DATABASE=PropDataMatchDB;uid=dataloader_prod;pwd=L@ttice2;";
    SQL_conn_leadscoring = "DRIVER={SQL Server};SERVER=10.41.1.187\sql2008r2;DATABASE=DataLoader;uid=dataloader_user;pwd=password;";
        
    #dataloader providers
    SQL_PropDataForModeling = "Data Source=bodcprodvsql130;" + \
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
    SQL_DanteDB_DataProvider = "Data Source=%s\SQL2012STD;" % pls_server + \
                        "Initial Catalog=DT_%s;" % pls_bard_1[3:]+ \
                        "Persist Security Info=True;" + \
                        "User ID=dataloader_prod;" + \
                        "Password=L@ttice2;"
                        
    SQL_ReportsDB_DataProvider_ELQ = "Data Source=%s\SQL2012STD;" % pls_server+ \
                        "Initial Catalog=%s;" % pls_bard_1+ \
                        "Persist Security Info=True;" + \
                        "User ID=dataloader_prod;" + \
                        "Password=L@ttice2;"
    
    SQL_ReportsDB_DataProvider_MKTO = "Data Source=%s\SQL2012STD;" % pls_server+ \
                        "Initial Catalog=%s;" % pls_bard_2+ \
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
                        
                        
   
    def __init__(self):
        '''
        Constructor
        '''
   



def main():
    pass
    
    

if __name__ == '__main__':
    main()