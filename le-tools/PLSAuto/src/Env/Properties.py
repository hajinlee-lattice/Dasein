#!/usr/local/bin/python
# coding: utf-8
'''
Created on 2015��2��28��

@author: GLiu
'''

class PLSEnvironments(object):
    
    pls_server="BODCDEVVINT82.dev.lattice.local";    
    pls_server_folder="D:\\B\\spda0__DE-DT-BD_SQ-d__6_9_2_64296nZ_2_6_2_63627n_1_3_4";
    pls_bard_1="BD_ADEDTBDd69264296nZ26263627n1";
    pls_bard_2="BD2_ADEDTBDd69264296nZ26263627n12";
    dl_server="https://bodcdevvint187.dev.lattice.local:8080/";
    visidb_server="bodcdevvint187.dev.lattice.local";
    
    
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
    
    dl_server_user="richard.liu@lattice-engines.com";
    dl_server_pwd="1";
    dl_dlc_path="%s\\%s\\ScoringDaemon\\ScoringDaemon\\bin\\Services\\DataLoaderShim" % (pls_server_folder, pls_bard_1[3:]);
    
    visidb_server_user="admin";
    visidb_server_pwd="visid@t@b@se";
    
    svn_location_local="D:\\prod\\PLSTemplate\\PLS";
    template_location="\\\\10.41.1.187\PLSTemplate";#"\\\\10.61.0.210\DevQA\share\PLSTemplate";
    
    pls_SFDC_user="apeters-widgettech@lattice-engines.com";
    pls_SFDC_pwd="Happy2010";
    pls_SFDC_key="oIogZVEFGbL3n0qiAp6F66TC";
    
    pls_ELQ_user="Matt.Sable";
    pls_ELQ_pwd="Lattice1";
    pls_ELQ_company="TechnologyPartnerLatticeEngines";
    
    pls_marketing_app_ELQ="ELQ";
    pls_marketing_app_MKTO="MKTO";
    
    SQL_JAMS_CFG = "DRIVER={SQL Server};SERVER=10.41.1.247;DATABASE=JAMSCFG;uid=dataloader_user;pwd=password";
    SQL_ScoringDaemon = "DRIVER={SQL Server};SERVER=%s\SQL2012STD;DATABASE=SD_%s;uid=dataloader_prod;pwd=L@ttice2" % (pls_server, pls_bard_1[3:]);
    
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
                        
   
    def __init__(self):
        '''
        Constructor
        '''
        