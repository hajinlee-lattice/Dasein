#!/usr/local/bin/python
# coding: utf-8

# Base test framework test cases

__author__ = "Illya Vinnichenko"
__copyright__ = "Copyright 2014"
__credits__ = ["Illya Vinnichenko"]
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "Illya Vinnichenko"
__email__ = "ivinnichenko@lattice-engines.com"
__status__ = "Alpha"

# import modules
import os
import platform
import requests
import json
import time
from copy import deepcopy
from datetime import datetime
from TestHelpers import BardAdminRunner
from TestHelpers import DLCRunner
from TestHelpers import EtlRunner
from TestHelpers import PretzelRunner
from TestHelpers import UtilsRunner
from TestHelpers import SessionRunner


#### PLS End-to-End pieces ####
def runLoadGroups(dlc, params, load_groups, max_run_time_in_sec=7200, sleep_time=300):
    command = "Launch Load Group"
    for lg in load_groups:
        params["-g"] = lg
        print "Running %s Load Group" % lg
        status = dlc.runDLCcommand(command, params)
        dlc.getStatus()
        creation_datetime = datetime.now()
        if not status:
            print "Load Group %s failed" % lg
            continue
        start = time.time()
        lg_status = "New"
        while(True):
            old_status = lg_status
            lg_status = getLoadGroupStatus(dlc, params, lg, creation_datetime)
            if lg_status == "Idle" and old_status == "Processing":
                print "Load Group %s status is Success" % lg
                break
            if lg_status == "Success":
                print "Load Group %s status is Success" % lg
                break
            if (time.time() - start) >= max_run_time_in_sec:
                print "Load Group %s did not succeed in %s seconds, moving on" % (lg, max_run_time_in_sec)
                break
            print "Load Group %s status is %s, will try again in %s seconds" % (lg, lg_status, sleep_time)
            time.sleep(sleep_time)

            

def getLoadGroupStatus(dlc, params, load_group, start_datetime):
    command = "Get Load Group Status"
    params["-g"] = load_group
    print load_group
    print dlc.runDLCcommand(command, params)
    text = dlc.request_text[-1].replace("<br/>", "\n")
    print text
    status = "Still working on it"
    for line in text.split("\n"):
        if line.startswith("State:"):
            #print line
            status = line[line.find("State:")+7:]
            print "%s load group is %s" % (load_group, status)
            if status == "Launch Succeeded":
                return "Success"
        if line.startswith("Last Succeeded:"):
            success = line[line.find("Last Succeeded:")+16:]
            print "%s load group last succeeded: %s" % (load_group, success)
            if success == "Never":
                return status
            else:
                try:
                    d = datetime.strptime(success, "%Y-%m-%d %H:%M:%S")
                    if start_datetime < d:
                        print "%s succeeded after the start date '%s' on '%s'" % (load_group,
                                                                                  start_datetime,
                                                                                  success)
                        return "Success"
                except ValueError:
                    print "Incorrectly formated string, should be YYYY-MM-DD HH:MM:SS"
                    return "Weird..."
                return status      
        

# Step 1
def setupPretzelTest(host, build_path, svn_location, topologies_list):
    location = "%s\PLS_Templates" % build_path
    pretzel = PretzelRunner(host=host, svn_location=svn_location, build_path=build_path, specs_path=location)
    pretzel.getMain()
    for topology in topologies_list:
        print pretzel.testTopology(topology)


# Step 2
def runEtl(host, marketting_app, tenant, sql_server, scoring_db, dante_db,
           svn_location="~/Code/Templates/PLS",
           etl_dir="C:\ETL"):
    etl = EtlRunner(svn_location, etl_dir, host)
    topology = ""
    if marketting_app == "Eloqua":
        topology = "EloquaAndSalesforceToEloqua"
    if marketting_app == "Marketo":
        topology = "MarketoAndSalesforceToMarketo"

    etl.generatePretzelEtlScript(tenant, marketting_app, sql_server, scoring_db, dante_db)
    etl.savePythonFile()
    etl.copyEtlFiles(topology)
    etl.runEtlEmulator(topology)
    etl.getStatus()
    config_string = etl.getConfigString(topology)
    # Should be a temporary fix, until main.py starts populating this field correctly
    if config_string.find('<DLConfigs appName="">') != -1:
        config_string = config_string.replace('<DLConfigs appName="">','<DLConfigs appName="%s">' % tenant)
    specs_string = etl.getSpecsString(topology)
    print len(config_string),len(specs_string)

    etl.write_to_file("output.config", config_string, True)
    etl.write_to_file("output.specs", specs_string, True)


def loadVisiDbConfigs(host, dlc_path, tenant,
                      dl_server="https://10.41.1.187:8080/",
                      user="Richard.liu@lattice-engines.com",
                      password="1",
                      config_file="output.config",
                      specs_file="output.specs",
                      config_dir="C:\CSV_TMP",
                      localCopy=False):

    dlc = DLCRunner(host=host, dlc_path=dlc_path)

    dlc.cpFile("output.config", config_dir, localCopy)
    dlc.cpFile("output.specs", config_dir, localCopy)

    command = "Migrate"
    params = {"-s": dl_server,
              "-u": user,
              "-p": password,
              "-t": tenant,
              "-mm": '"replace"'
             }
    params["-cfp"] = '"%s\%s"' % (config_dir, config_file)
    params["-sfp"] = '"%s\%s"' % (config_dir, specs_file)
    dlc.runDLCcommand(command, params)
    dlc.getStatus()


def configurePLSCredentialsTest(host, dlc_path, tenant, marketting_app,
                                dl_server="https://10.41.1.187:8080/",
                                user="Richard.liu@lattice-engines.com",
                                password="1"):

    dlc = DLCRunner(host=host, dlc_path=dlc_path)
    command = "New Data Provider"
    params = {"-s": dl_server,
              "-u": user,
              "-p": password,
              "-t": tenant,
              "-dpf": '"upload|validation extract|leaf extract|itc|fstable"',
              "-v": "true"
             }
    # SFDC
    connection_string = '"User=apeters-widgettech@lattice-engines.com;' + \
                        'Password=Happy2010;' + \
                        'SecurityToken=uMWcnIq9rCOxtRteKKgixE26;' + \
                        'Timeout=100;RetryTimesForTimeout=3;BatchSize=2000"'
    params["-cs"] = connection_string
    params["-dpn"] = "SFDC_DataProvider"
    params["-dpt"] = "sfdc"
    print "SFDC"
    dlc.runDLCcommand(command, params)
    dlc.getStatus()

    # Marketo
    if marketting_app == "Marketo":
        connection_string = '"URL=https://na-sj02.marketo.com/soap/mktows/2_0;"' + \
                            'UserID=latticeenginessandbox1_9026948050BD016F376AE6;' + \
                            'EncryptionKey=41802295835604145500BBDD0011770133777863CA58;' + \
                            'Timeout=10000;RetryTimesForTimeout=3;SleepTimeBeforeRetry=60;' + \
                            'BatchSize=500;MaxSizeOfErrorBatch=25;"'
        params["-cs"] = connection_string
        params["-dpn"] = "Maketo_DataProvider"
        params["-dpt"] = "marketo"
        print "Marketo"
        dlc.runDLCcommand(command, params)
        dlc.getStatus()

    # Eloqua
    if marketting_app == "Eloqua":
        connection_string = '"URL=https://login.eloqua.com/id;' + \
                            'Company=TechnologyPartnerLatticeEngines;' + \
                            'Username=Matt.Sable;' + \
                            'Password=Lattice1;' + \
                            'APIType=SOAP;EntityType=Base;Timeout=300;RetryTimesForTimeout=3;' + \
                            'SleepTimeBeforeRetry=60;BatchSize=200;MaxSizeOfErrorBatch=25;"'
        params["-cs"] = connection_string
        params["-dpn"] = "Eloqua_DataProvider"
        params["-dpt"] = "eloqua"
        print "Eloqua"
        dlc.runDLCcommand(command, params)
        dlc.getStatus()

# Step 2.5
def editDataProviders(host, dlc_path, tenant, dp, connection_string,
                      dl_server="https://10.41.1.187:8080/",
                      user="Richard.liu@lattice-engines.com",
                      password="1"):

    dlc = DLCRunner(host=host, dlc_path=dlc_path)
    command = "Edit Data Provider"
    params = {"-s": dl_server,
              "-u": user,
              "-p": password,
              "-t": tenant,
              "-v": "true"
             }
    params["-cs"] = '"%s"' % connection_string
    params["-dpn"] = dp
    print dp
    dlc.runDLCcommand(command, params)
    dlc.getStatus()

def configDLTables(host, dlc_path, tenant,
                    dl_server="https://10.41.1.187:8080/",
                    user="Richard.liu@lattice-engines.com",
                    password="1"):

    # SQL_PropDataForModeling
    connection_string = "Data Source=bodcprodvsql130;" + \
                        "Initial Catalog=PropDataMatchDB;" + \
                        "Authentication=SQL Server Authentication;" + \
                        "Persist Security Info=True;" + \
                        "User ID=dataloader_prod;" + \
                        "Password=L@ttice2;"

    editDataProviders(host, dlc_path, tenant, "SQL_PropDataForModeling", connection_string,
                      dl_server, user, password)

    # SQL_LeadScoring
    connection_string = "Data Source=10.41.1.187\sql2008r2;" + \
                        "Initial Catalog=LeadScoringDB;" + \
                        "Authentication=SQL Server Authentication;" + \
                        "Persist Security Info=True;" + \
                        "User ID=dataloader_user;" + \
                        "Password=password;"
    editDataProviders(host, dlc_path, tenant, "SQL_LeadScoring", connection_string,
                      dl_server, user, password)


def createMockDataProviders(host, dlc_path, tenant, marketting_app,
                            dl_server="https://10.41.1.187:8080/",
                            user="Richard.liu@lattice-engines.com",
                            password="1"):

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
    init_catalog = "PLS_MKTO_SFDC_ReleaseQA_20150630"
    market_dp = "Mock_Marketo_Data_Provider"
    if marketting_app == "Eloqua":
        init_catalog = "PLS_MKTO_SFDC_ReleaseQA_20150630"
        market_dp = "Mock_Eloqua_Data_Provider"

    connection_string = '"Data Source=10.41.1.187\sql2008;' + \
                        'Initial Catalog=%s;' % init_catalog + \
                        'Persist Security Info=True;' + \
                        "User ID=dataloader_user;" + \
                        "Password=password;"
    params["-cs"] = connection_string
    params["-dpn"] = "Mock_SFDC_Data_Provider"
    params["-dpt"] = "sqlserver"
    print "Mock SFDC"
    dlc.runDLCcommand(command, params)
    dlc.getStatus()

    # Mock Marketting App
    connection_string = '"Data Source=10.41.1.187\sql2008;' + \
                        'Initial Catalog=%s;' % init_catalog + \
                        'Persist Security Info=True;' + \
                        "User ID=dataloader_user;" + \
                        "Password=password;"
    params["-cs"] = connection_string
    params["-dpn"] = market_dp
    params["-dpt"] = "sqlserver"
    print "Mock %s" % marketting_app
    dlc.runDLCcommand(command, params)
    dlc.getStatus()

def editMockRefreshDataSources(host, dlc_path, tenant, marketting_app,
                               dl_server="https://10.41.1.187:8080/",
                               user="Richard.liu@lattice-engines.com",
                               password="1"):

    dlc = DLCRunner(host=host, dlc_path=dlc_path)
    command = "Edit Refresh Data Source"
    params = {"-s": dl_server,
              "-u": user,
              "-p": password,
              "-t": tenant,
              "-f": '"@recordcount(1000000)"'
             }
    
    #LoadCRMDataForModeling
    rds_list = ["SFDC_User", "SFDC_Contact", "SFDC_Lead", "SFDC_Opportunity", "SFDC_OpportunityContactRole"]
    for rds in rds_list:
        params["-g"] = "LoadCRMDataForModeling"
        params["-rn"] = rds
        params["-cn"] = "Mock_SFDC_Data_Provider"
        print "Updating Refresh Data Source %s for Mock_SFDC_Data_Provider" % rds
        dlc.runDLCcommand(command, params)
        dlc.getStatus()

    #LoadMAPDataForModeling
    if marketting_app == "Eloqua":
        params["-g"] = "LoadMAPDataForModeling"
        params["-rn"] = "ELQ_Contact"
        params["-cn"] = "Mock_Eloqua_Data_Provider"
        print "Updating Refresh Data SourceELQ_Contact for Mock_Eloqua_Data_Provider"
        dlc.runDLCcommand(command, params)
        dlc.getStatus()
    elif marketting_app == "Marketo":
        params["-cn"] = "Mock_Marketo_Data_Provider"
        rds_dict = {"LoadMAPDataForModeling_ActivityRecord_NewLead": "ActivityRecord_NewLead",
                    "LoadMAPDataForModeling_ActivityRecord_OtherThanNewLead": "ActivityRecord_OtherThanNewLead",
                    "LoadMAPDataForModeling_LeadRecord": "MKTO_LeadRecord"}
        for lg in rds_dict:
            params["-g"] = lg
            params["-rn"] = rds_dict[lg]
            print "Updating Refresh Data Source %s for Mock_Marketo_Data_Provider" % rds_dict[lg]
            dlc.runDLCcommand(command, params)
            dlc.getStatus()
    else:
        print "!!![%s] MARKETTING UP IS NOT SUPPORTED!!!" % marketting_app


# Step 2.75
def loadCfgTablesTest(dlc_host, dlc_path, tenant, svn_location, dp_folder,
                      marketting_apps_list=["Marketo", "Eloqua"],
                      local=False, cp=True,
                      dl_server="https://10.41.1.187:8080/",
                      user="Richard.liu@lattice-engines.com",
                      password="1"):

    dlc = DLCRunner(host=dlc_host, dlc_path=dlc_path)
    params = {"-s": dl_server,
              "-u": user,
              "-p": password,
              "-t": tenant
             }

    runLoadGroups(dlc, params, ["ImportMetaData"], sleep_time=30)
    dlc.getStatus()

    utils = UtilsRunner(host=dlc_host)
    for marketting_app in marketting_apps_list:
        # Pre-process templates
        location = os.path.join(svn_location, "%s csv files for QA to load standard Cfg tables" % marketting_app)
        files = utils.getFiles(location, [".csv"])
        schema_map = utils.createSchemaDir(location, files, marketting_app)
        reloc_dir = utils.relocateCsvFile(dp_folder, schema_map, marketting_app, local, cp)

        # Create New Load Group
        print "Creating New Load Group"
        lg_name = "Group_LoadCfgTables_%s" % marketting_app
        lg_params = deepcopy(params)
        lg_params["-g"] = lg_name
        #dlc.constructCommand("New Load Group", lg_params)
        dlc.runDLCcommand("New Load Group", lg_params)
        dlc.getStatus()

        # Create new Data Provider for each Schema:
        print "Creating new Data Provider for each Schema"
        command = "New Data Provider"
        dp_params = deepcopy(params)
        dp_params["-dpf"] = "upload"
        dp_params["-v"] = "true"
        dp_params["-dpt"] = "sftp"
        
        for schema in reloc_dir:
            dp_params["-dpn"] = "%s_%s_DataProvider" % (marketting_app, schema)
            dp_location = reloc_dir[schema].replace("/", "\\")
            dp_params["-cs"] = '"File=%s;BatchSize=5000"' % dp_location
            #dlc.constructCommand(command, dp_params)
            print dlc.runDLCcommand(command, dp_params)
            dlc.getStatus()

        # Create new Refresh Data Source for each Schema:
        print "Creating new Refresh Data Source for each Schema"
        command = "New Refresh Data Source"
        rd_params = deepcopy(params)
        rd_params["-g"] = lg_name

        for schema in reloc_dir:
            data_provider = "%s_%s_DataProvider" % (marketting_app, schema)
            rd_params["-rn"] = "%s_RDS" % data_provider
            rd_params["-sn"] = schema
            rd_params["-cn"] = data_provider
            #dlc.constructCommand(command, rd_params)
            dlc.runDLCcommand(command, rd_params)
            dlc.getStatus()

        runLoadGroups(dlc, params, [lg_name])

    # Upload the files into DL...
    


# Step 3
def runModelingLoadGroups(dlc_host, dlc_path, tenant,
                          dl_server="https://10.41.1.187:8080/",
                          user="Richard.liu@lattice-engines.com",
                          password="1",
                          step_by_step=False):

    dlc = DLCRunner(host=dlc_host, dlc_path=dlc_path)
    params = {"-s": dl_server,
              "-u": user,
              "-p": password,
              "-t": tenant
             }
    if step_by_step:
        load_groups = ["LoadMapDataForModeling",
                       "LoadCRMDataForModeling",
                       "ModelBuild_PropDataMatch",
                       "CreateEventTableQueries",
                       "CreateAnalyticPlay"]
        runLoadGroups(dlc, params, load_groups)
    else:
        runLoadGroups(dlc, params, ["ExecuteModelBuilding"])

def getModelFromModelingServiceTest(bard_host, bard_path, second_bard_path, second_bard_tenant=False):
    # Run AutomaticModelDownloader
    bard = BardAdminRunner(host=bard_host, bard_path=bard_path)
    if second_bard_tenant:
        print "Re-Configuring BardAdminTool"
        bard.bard_path = second_bard_path
    bard.runSetProperty("ModelDownloadSettings", "HDFSNameServerAddress", "10.41.1.216")
    bard.getStatus()
    bard.runSetProperty("ModelDownloadSettings", "HDFSNameServerPort", 50070)
    bard.getStatus()
    print "Model Download Settings"
    bard.runGetDocument("ModelDownloadSettings")
    bard.getStatus()
    print "Models available for Download"
    bard.runListModels()
    bard.getStatus()
    bard.getLatestModelInfo()
    bard.getStatus()
    print "Force Model Download"
    bard.runDownloadModels()
    bard.getStatus()

# Step 4
# BODCDEPVJOB247.prod.lattice.local
# Deal with JAMS

# Step 5
def activateModelTest(bard_host, bard_path, second_bard_path,
                      use_second_bard_tenant=False):

    bard_host = bard_host
    bard = BardAdminRunner(host=bard_host, bard_path=bard_path)
    if use_second_bard_tenant:
        print "Re-Configuring BardAdminTool"
        bard.bard_path = second_bard_path
        bard.runSetProperty("ScoringConfiguration", "LeadsPerBatch", 10000)
        bard.getStatus()
        bard.runSetProperty("ScoringConfiguration", "LeadsPerHour", 50000)
        bard.getStatus()
    "Models available for Download"
    bard.runListModels()
    bard.getStatus()
    model_id = bard.getLatestModelId()
    print "Activate Model"
    bard.runActivateModel(model_id)
    bard.getStatus()

# Step 6
def waitForLeadInputQueue(sql_server, db_name):
    # Wait for the leads to be scored
    dlc = SessionRunner()
    connection_string = "DRIVER={SQL Server};SERVER=%s;DATABASE=%s;uid=dataloader_user;pwd=password" % (sql_server, db_name)
    if platform.system() == "Linux":
        connection_string = "DRIVER=FreeTDS;SERVER=%s;DATABASE=%s;uid=dataloader_user;pwd=password" % (sql_server, db_name)
    query = "select * from dbo.LeadInputQueue"
    #query = "SELECT * FROM information_schema.tables"
    wait_cycle = 0
    print connection_string
    print query
    while(wait_cycle < 1):
        results = dlc.getQuery(connection_string, query)
        if results:
            #print results
            result = results[-1]
            if len(result) < 3:
                print "Uknown query result: %s" % result
            else:
                res = result[-3]
                print "Status: for %s is %s" % (result[2], res)
                if res == 2:
                    print "Job succeeded!"
                    break
        print "Sleeping for 180 seconds, hoping Status turns 2"
        wait_cycle += 1
        time.sleep(180)

def doScoring(dlc_host, dlc_path, tenant, sql_server, db_name,
              dl_server="https://10.41.1.187:8080/",
              user="Richard.liu@lattice-engines.com",
              password="1"):

    pre_scoring_list = ["LoadCRMData", "LoadMapData", "PropDataMatch", "PushToScoringDB"]
    post_scoring_list = ["PushToLeadDestination", "PushToReportsDB", "ConsolidateExtracts"]
    dlc = DLCRunner(host=dlc_host, dlc_path=dlc_path)
    params = {"-s": dl_server,
              "-u": user,
              "-p": password,
              "-t": tenant
             }
    # Run PreScoring loadgroups:
    runLoadGroups(dlc, params, pre_scoring_list, sleep_time=60)

    # Wait for the leads to be scored
    waitForLeadInputQueue(sql_server, db_name)

    # Run PostScoring loadgroups:
    runLoadGroups(dlc, params, post_scoring_list)

# Step 7
# Push stuff to Dante


######################################################################

##################### PLS End-to-End test ########################

def runPlsEndToEndTest(marketting_app, run_setup=False, run_test=True, use_second_bard=False):
    print "End to End Starts now!"

##################### PLS End-to-End test ########################

def testSetup(marketting_app, run_setup=False, run_test=True, use_second_bard=False):
    # These parameters should be taken from Jenkins build info page

    ############ Generic Parameters ######################
    # this parameter should be in format "http://ip:5000" IP, NOT the name!!!
    # for ex. for bodcdevvint57.dev.lattice.local it should be "http://10.41.1.57:5000"
    pls_test_server = "http://10.41.1.66:5000"

    # this parameter should be in format "http://ip:5000" IP, NOT the name!!!
    # for ex. for bodcdevvint57.dev.lattice.local it should be "http://10.41.1.187:5000"
    dlc_test_server = "http://10.41.1.187:5000"

    # Generic path for AUT locatio, for example:
    # D:\B\spda0__DE-DT-BD_SQ-d__7_0_0_64254nD_2_6_1_63627r_1_3_3_0c\ADEDTBDd70064254nD26163627r1
    install_dir = "D:\B\spda0__DE-DT-BD_SQ-d__6_9_2_64090rC_2_6_2_63627n_1_3_4\ADEDTBDd69264090rC26263627n1"

    # Server name for SOAP url generation:
    soap_host = "BODCDEVVQAP25.dev.lattice.local"

    # Local svn location to grab the required templates from
    svn_location = "~/Code/Templates/PLS"
    #svn_location = "~/Code/PLS_1.3.3/Templates"

    # Tenant for BARD
    tenant = "BD_ADEDTBDd69264090rC26263627n1"

    # Tenant for BARD2
    tenant_2 = "BD2_ADEDTBDd69264090rC26263627n12"
    #######################################################

    ############ Pretzel Parameters ######################
    # Topologies to test
    topologies_list = []
    if marketting_app == "Marketo":
        topologies_list.append("MarketoAndSalesforceToMarketo")
    if marketting_app == "Eloqua":
        topologies_list.append("EloquaAndSalesforceToEloqua")

    ############ BARD Parameters ######################
    # Bard installation path
    bard_path = "%s\Bard\Install\\bin" % install_dir
    # Bard 2 installation path
    bard_path_2 = "%s2\Bard2\Install\\bin" % install_dir

    ############ DLC Parameters ######################
    # VisiDB path
    dlc_path = "D:\VisiDB"
    dp_folder = "C:\CSV_TMP"
    etl_dir = "C:\ETL"
    load_csv_for = []
    if marketting_app == "Marketo":
        load_csv_for.append("Marketo")
    if marketting_app == "Eloqua":
        load_csv_for.append("Eloqua")

    # SQL Server params
    sql_server = "BODCDEVVCUS66.dev.lattice.local\SQL2012STD"
    scoring_db_name = "SD_ADEDTBDd69264090rC26263627n1"
    dante_db_name = "DT_ADEDTBDd69264090rC26263627n1"

    # Start execution
    tenant_to_use = tenant
    if use_second_bard:
        tenant_to_use = tenant_2
        #install_dir = install_dir + "2"

    # Initial Setup, should be run only once
    if run_setup:
        # Refresh SVN
        print "Refreshing SVN"
        runner = SessionRunner()
        print runner.runCommandLocally("svn update", svn_location)

        print "Running Setup"
        # Step 1 - Setup Pretzel
        setupPretzelTest(pls_test_server, install_dir, svn_location, topologies_list)
        
        # Step 2 - Configure PLS Credentials
        runEtl(pls_test_server, marketting_app, tenant_to_use,
               sql_server, scoring_db_name, dante_db_name,
               svn_location, etl_dir)
        loadVisiDbConfigs(dlc_test_server, dlc_path, tenant_to_use)

        # Step 2.5 - Configure DL tables
        configDLTables(dlc_test_server, dlc_path, tenant_to_use)

        # Create Mock Data Providers
        createMockDataProviders(dlc_test_server, dlc_path, tenant_to_use, marketting_app)
        editMockRefreshDataSources(dlc_test_server, dlc_path, tenant_to_use, marketting_app)

        # Step 2.75 - Need to rework
        loadCfgTablesTest(dlc_test_server, dlc_path, tenant_to_use, svn_location, dp_folder, load_csv_for)

    # Now the actual testing starts

    if run_test:
        # Step 3 - Run LoadGroups and Download Model
        
        runModelingLoadGroups(dlc_test_server, dlc_path, tenant_to_use)
        getModelFromModelingServiceTest(pls_test_server, bard_path, bard_path_2,
                                        second_bard_tenant=use_second_bard)
    
        # Step 4 - Deal with JAMS (only for Dante - WIP)
    
        # Step 5 - Activate the Model
        activateModelTest(pls_test_server, bard_path, bard_path_2,
                          use_second_bard_tenant=use_second_bard)
    
        """
        """
        # Steps 6 - Not really a step, let's keep the slot for verification
        doScoring(dlc_test_server, dlc_path, tenant, sql_server, scoring_db_name)
    
        # Step 7 - Upload to Dante and Deal with that - WIP


def main():
    print "Welcome to PLS End to End Automation!"
    testSetup("Eloqua", run_setup=False, use_second_bard=False, run_test=True)
    #testSetup("Marketo", run_setup=False, use_second_bard=True, run_test=True)


if __name__ == '__main__':
    main()