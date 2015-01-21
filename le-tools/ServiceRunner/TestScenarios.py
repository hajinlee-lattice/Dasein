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
#from TestConfigs import ConfigDLC
#from TestRunner import SessionRunner
from TestHelpers import PretzelRunner
from TestHelpers import DLCRunner
from TestHelpers import BardAdminRunner

def testHelperClasses():
    DLCRunner().testRun()

def basePretzelTest():
    bp = "D:\B\spda0__DE-DT-BD_SQ-d__6_9_2_63971nE_2_6_1_63627r_1_3_3_0n\ADEDTBDd69263971nE26163627r1"
    location = "%s\PLS_Templates" % bp
    pretzel = PretzelRunner(host="http://10.41.1.57:5000",
                            svn_location="~/Code/PLS_1.3.3", build_path=bp, specs_path=location)
    pretzel.getMain()
    # Test "EloquaAndSalesforceToEloqua"
    pretzel.testTopology("EloquaAndSalesforceToEloqua")
    # Test "MarketoAndSalesforceToMarketo"
    pretzel.testTopology("MarketoAndSalesforceToMarketo")

def baseDLCTest():
    host = "http://10.41.1.187:5000"
    command = "Launch"
    params = {"-s": "https://10.41.1.187:8080/",
              "-u": "Richard.liu@lattice-engines.com ",
              "-p": "1",
              "-li": "7"
             }
    dlc = DLCRunner(host=host, dlc_path="D:\VisiDB")
    print dlc.runDLCcommand(command, params)
        
#### PLS End-to-End pieces ####

# Step 1
def setupPretzelTest(host, build_path, svn_location, topologies_list):
    location = "%s\PLS_Templates" % build_path
    pretzel = PretzelRunner(host=host, svn_location=svn_location, build_path=build_path, specs_path=location)
    pretzel.getMain()
    for topology in topologies_list:
        print pretzel.testTopology(topology)


# Step 2
def configurePLSCredentialsTest(host, dlc_path, tenant,
                                dl_server="https://10.41.1.187:8080/",
                                user="Richard.liu@lattice-engines.com",
                                password="1"):

    dlc = DLCRunner(host=host, dlc_path=dlc_path)
    command = "New Data Provider"
    params = {"-s": dl_server,
              "-u": user,
              "-p": password,
              "-t": tenant,
              "-dpf": "upload"
             }
    # SFDC
    connection_string = "'User=apeters-widgettech@lattice-engines.com;" + \
                        "Password=Happy2010;" + \
                        "SecurityToken=uMWcnIq9rCOxtRteKKgixE26;" + \
                        "Timeout=100;RetryTimesForTimeout=3;BatchSize=2000'"
    params["-cs"] = connection_string
    params["-dpn"] = "SFDC_DataProvider"
    params["-dpt"] = "sfdc"
    print "SFDC"
    print dlc.runDLCcommand(command, params)

    # Marketo
    connection_string = "URL=https://na-sj02.marketo.com/soap/mktows/2_0;" + \
                        "UserID=latticeenginessandbox1_9026948050BD016F376AE6;" + \
                        "EncryptionKey=41802295835604145500BBDD0011770133777863CA58;" + \
                        "Timeout=10000;RetryTimesForTimeout=3;SleepTimeBeforeRetry=60;" + \
                        "BatchSize=500;MaxSizeOfErrorBatch=25;"
    params["-cs"] = connection_string
    params["-dpn"] = "Maketo_DataProvider"
    params["-dpt"] = "marketo"
    print "Marketo"
    print dlc.runDLCcommand(command, params)

    # Eloqua
    connection_string = "URL=https://login.eloqua.com/id;" + \
                        "Company=TechnologyPartnerLatticeEngines;" + \
                        "Username=Matt.Sable;" + \
                        "Password=Lattice1;" + \
                        "APIType=SOAP;EntityType=Base;Timeout=300;RetryTimesForTimeout=3;" + \
                        "SleepTimeBeforeRetry=60;BatchSize=200;MaxSizeOfErrorBatch=25;"
    params["-cs"] = connection_string
    params["-dpn"] = "Eloqua_DataProvider"
    params["-dpt"] = "eloqua"
    print "Eloqua"
    print dlc.runDLCcommand(command, params)

# Step 2.5
def configureDLTest(host, dlc_path, tenant,
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

    # SQL_PropDataForModeling
    # TODO: verify correct "Initial Catalog" Value
    connection_string = "Data Source= bodcprodvsql130;" + \
                        "Initial Catalog=testdb;" + \
                        "Persist Security Info=True;" + \
                        "User ID=dataloader_prod;" + \
                        "Password=L@ttice2;"
    params["-cs"] = connection_string
    params["-dpn"] = "SQL_PropDataForModeling"
    print "SQL_PropDataForModeling"
    print dlc.runDLCcommand(command, params)

    # SQL_LeadScoring
    # TODO: verify correct "Initial Catalog" Value
    connection_string = "Data Source=10.41.1.187\sql2008r2;" + \
                        "Initial Catalog=testdb;" + \
                        "Persist Security Info=True;" + \
                        "User ID=dataloader_user;" + \
                        "Password=password;"
    params["-cs"] = connection_string
    params["-dpn"] = "SQL_LeadScoring"
    print "SQL_LeadScoring"
    print dlc.runDLCcommand(command, params)

# Step 2.75
# WIP - needs to be re-worked
def loadCfgTablesTest():
    host = "http://10.41.1.187:5000"

    svn_location = ""
    marketo_location = os.path.join(svn_location,"Templates", "Marketo csv files for QA to load standard Cfg tables")
    eloqua_location = os.path.join(svn_location,"Templates", "Eloqua csv files for QA to load standard Cfg tables")

    dlc = DLCRunner(host=host, dlc_path="D:\VisiDB")
    marketo_files = dlc.getFiles(marketo_location, [".csv"])
    eloqua_files = dlc.getFiles(eloqua_location, [".csv"])
    print marketo_files, eloqua_files
    # Upload the files into DL...


# Step 3
def runLoadGroups(dlc, params, load_groups):
    command = "Launch Load Group"
    for lg in load_groups:
        params["-g"] = lg
        print lg
        print dlc.runDLCcommand(command, params)

def getModelFromModelingServiceTest(dlc_host, dlc_path, tenant,
                                    bard_host, bard_path, second_bard_path, 
                                    dl_server="https://10.41.1.187:8080/",
                                    user="Richard.liu@lattice-engines.com",
                                    password="1",
                                    step_by_step=False, second_bard_tenant=False):

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

    # Run AutomaticModelDownloader
    bard = BardAdminRunner(host=bard_host, bard_path=bard_path)
    if second_bard_tenant:
        print "Re-Configuring BardAdminTool"
        bard.bard_path = second_bard_path
        print bard.runSetProperty("ModelDownloadSettings", "HDFSNameServerAddress", "10.41.1.216")
        print bard.runSetProperty("ModelDownloadSettings", "HDFSNameServerPort", 50070)
    print "Model Download Settings"
    print bard.runGetDocument("ModelDownloadSettings")
    print "Models available for Download"
    print bard.runListModels()
    print "Force Model Download"
    print bard.runDownloadModels

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
        print bard.runSetProperty("ScoringConfiguration", "LeadsPerBatch", 10000)
        print bard.runSetProperty("ScoringConfiguration", "LeadsPerHour", 50000)
    print "Models available for Download"
    print bard.runListModels()
    # TODO: Get an ID from that List (parse the prev. command stdout)
    model_id = ""
    print "Activate Model"
    print bard.runActivateModel(model_id)

# Step 6
# Not really a step, used for debugging

# Step 7
# Push stuff to Dante


######################################################################

##################### PLS End-to-End test ########################

def runPlsEndToEndTest(run_setup=False, use_second_bard=False):
    # These parameters should be taken from Jenkins build info page

    ############ Generic Parameters ######################
    # this parameter should be in format "http://ip:5000" IP, NOT the name!!!
    # for ex. for bodcdevvint57.dev.lattice.local it should be "http://10.41.1.57:5000"
    pls_test_server = "http://10.41.1.57:5000"

    # this parameter should be in format "http://ip:5000" IP, NOT the name!!!
    # for ex. for bodcdevvint57.dev.lattice.local it should be "http://10.41.1.187:5000"
    dlc_test_server = "http://10.41.1.187:5000"

    # Generic path for AUT locatio, for example:
    # D:\B\spda0__DE-DT-BD_SQ-d__6_9_2_64090rQ_2_6_1_63627r_1_3_3_0n\ADEDTBDd69264090rQ26163627r1
    install_dir = "D:\B\spda0__DE-DT-BD_SQ-d__6_9_2_64090rQ_2_6_1_63627r_1_3_3_0n\ADEDTBDd69264090rQ26163627r1"

    # Local svn location to grab the required templates from
    svn_location = "~/Code/PLS_1.3.3"

    # Tenant for BARD
    tenant = "BD_ADEDTBDd69264090rQ26163627r1"

    # Tenant for BARD2
    tenant_2 = "BD_ADEDTBDd69264090rQ26163627r12"
    #######################################################

    ############ Pretzel Parameters ######################
    # Topologies to test
    topologies_list = ["EloquaAndSalesforceToEloqua", "MarketoAndSalesforceToMarketo"]

    ############ BARD Parameters ######################
    # Bard installation path
    bard_path = "%s\Bard\Install\bin" % install_dir
    # Bard 2 installation path
    bard_path_2 = "%s2\Bard2\Install\bin" % install_dir

    ############ DLC Parameters ######################
    # VisiDB path
    dlc_path = "D:\VisiDB"

    # Start execution
    tenant_to_use = tenant
    if use_second_bard:
        tenant_to_use = tenant_2

    # Initial Setup, should be run only once
    if run_setup:
        # Step 1 - Setup Pretzel
        setupPretzelTest(pls_test_server, install_dir, svn_location, topologies_list)
        
        # Step 2 - Configure PLS Credentials
        configurePLSCredentialsTest(dlc_test_server, dlc_path, tenant_to_use)

        # Step 2.5 - Configure DL tables
        configureDLTest(dlc_test_server, dlc_path, tenant_to_use)

        # Step 2.75 - Need to rework

    # Now the actual testing starts
    
    # Step 3 - Run LoadGroups and Download Model
    getModelFromModelingServiceTest(dlc_test_server, dlc_path, tenant_to_use,
                                    pls_test_server, bard_path, bard_path_2,
                                    step_by_step=False, second_bard_tenant=use_second_bard)

    # Step 4 - Deal with JAMS (only for Dante - WIP)
    
    # Step 5 - Activate the Model
    activateModelTest(pls_test_server, bard_path, bard_path_2,
                      second_bard_tenant=use_second_bard)

    # Steps 6 - Not really a step, let's keep the slot for verification

    # Step 7 - Upload to Dante and Deal with that - WIP


######################################################################

##################### PLS End-to-End test ########################

def testSetup(run_setup=False, use_second_bard=False):
    # These parameters should be taken from Jenkins build info page

    ############ Generic Parameters ######################
    # this parameter should be in format "http://ip:5000" IP, NOT the name!!!
    # for ex. for bodcdevvint57.dev.lattice.local it should be "http://10.41.1.57:5000"
    pls_test_server = "http://10.41.1.57:5000"

    # this parameter should be in format "http://ip:5000" IP, NOT the name!!!
    # for ex. for bodcdevvint57.dev.lattice.local it should be "http://10.41.1.187:5000"
    dlc_test_server = "http://10.41.1.187:5000"

    # Generic path for AUT locatio, for example:
    # D:\B\spda0__DE-DT-BD_SQ-d__6_9_2_64090rQ_2_6_1_63627r_1_3_3_0n\ADEDTBDd69264090rQ26163627r1
    install_dir = "D:\B\spda0__DE-DT-BD_SQ-d__6_9_2_64090rQ_2_6_1_63627r_1_3_3_0n\ADEDTBDd69264090rQ26163627r1"

    # Local svn location to grab the required templates from
    svn_location = "~/Code/PLS_1.3.3"

    # Tenant for BARD
    tenant = "BD_ADEDTBDd69264090rQ26163627r1"

    # Tenant for BARD2
    tenant_2 = "BD_ADEDTBDd69264090rQ26163627r12"
    #######################################################

    ############ Pretzel Parameters ######################
    # Topologies to test
    topologies_list = ["EloquaAndSalesforceToEloqua", "MarketoAndSalesforceToMarketo"]

    ############ BARD Parameters ######################
    # Bard installation path
    bard_path = "%s\Bard\Install\\bin" % install_dir
    # Bard 2 installation path
    bard_path_2 = "%s2\Bard2\Install\\bin" % install_dir

    ############ DLC Parameters ######################
    # VisiDB path
    dlc_path = "D:\VisiDB"

    bard = BardAdminRunner(host=pls_test_server, bard_path=bard_path)
    print "Models available for Download"
    print bard.runListModels()
    print bard.request_text
    # TODO: Get an ID from that List (parse the prev. command stdout)
    model_id = ""


def main():
    print "Welcome to TestScenarios!"
    testSetup()
    # Basic Pretzel Test case
    #basePretzelTest()

    # Basic DLC Test case
    #baseDLCTest()

    # Sample DLC unittest
    #DLCRunner().testRun()


if __name__ == '__main__':
    main()