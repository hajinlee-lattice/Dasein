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

def runLoadGroups(dlc, params, load_groups):
    command = "Launch Load Group"
    for lg in load_groups:
        params["-g"] = lg
        print lg
        print dlc.runDLCcommand(command, params)
        
#### PLS End-to-End pieces ####

# Step 1
def setupPretzelTest():
    bp = "D:\B\spda0__DE-DT-BD_SQ-d__6_9_2_63971nE_2_6_1_63627r_1_3_3_0n\ADEDTBDd69263971nE26163627r1"
    location = "%s\PLS_Templates" % bp
    pretzel = PretzelRunner(host="http://10.41.1.57:5000",
                            svn_location="~/Code/PLS_1.3.3", build_path=bp, specs_path=location)
    pretzel.getMain()
    # Test "EloquaAndSalesforceToEloqua"
    pretzel.testTopology("EloquaAndSalesforceToEloqua")
    # Test "MarketoAndSalesforceToMarketo"
    pretzel.testTopology("MarketoAndSalesforceToMarketo")

# Step 2
# TODO: verify correct "Tenant Name" value
def configurePLSCredentialsTest():
    host = "http://10.41.1.187:5000"
    dlc = DLCRunner(host=host, dlc_path="D:\VisiDB")
    command = "New Data Provider"
    params = {"-s": "https://10.41.1.187:8080/",
              "-u": "Richard.liu@lattice-engines.com ",
              "-p": "1",
              "-t": "????",
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
# TODO: verify correct "Tenant Name" value
def configureDLTest():
    host = "http://10.41.1.187:5000"
    dlc = DLCRunner(host=host, dlc_path="D:\VisiDB")
    command = "Edit Data Provider"
    params = {"-s": "https://10.41.1.187:8080/",
              "-u": "Richard.liu@lattice-engines.com ",
              "-p": "1",
              "-t": "????",
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
    params["-dpn"] = "SQL_PropDataForModeling"
    print "SQL_PropDataForModeling"
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
    # Upload the files into DL...


# Step 3
def getModelFromModelingServiceTest(step_by_step=False, second_bard_tenant=False):
    # Launch Load Group(s) in DL
    dlc_host = "http://10.41.1.187:5000"
    dlc = DLCRunner(host=dlc_host, dlc_path="D:\VisiDB")
    params = {"-s": "https://10.41.1.187:8080/",
              "-u": "Richard.liu@lattice-engines.com ",
              "-p": "1",
              "-t": "????",
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
    bard_host = "http://10.41.1.57:5000"
    bard = BardAdminRunner(host=bard_host, bard_path="~/Code/PLS_1.3.3")
    if second_bard_tenant:
        print "Re-Configuring BardAdminTool"
        bard.bard_path = ""
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
def activateModelTest(second_bard_tenant=False):
    bard_host = "http://10.41.1.57:5000"
    bard = BardAdminRunner(host=bard_host, bard_path="~/Code/PLS_1.3.3")
    if second_bard_tenant:
        print "Re-Configuring BardAdminTool"
        bard.bard_path = ""
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


def main():
    print "Welcome to TestScenarios!"
    # Basic Pretzel Test case
    #basePretzelTest()

    # Basic DLC Test case
    #baseDLCTest()

    # Sample DLC unittest
    #DLCRunner().testRun()


if __name__ == '__main__':
    main()