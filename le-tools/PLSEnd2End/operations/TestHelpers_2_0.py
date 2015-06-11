#!/usr/local/bin/python
# coding: utf-8

# Base test framework test helpers

__author__ = "Illya Vinnichenko"
__copyright__ = "Copyright 2014"
__credits__ = ["Illya Vinnichenko"]
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "Illya Vinnichenko"
__email__ = "ivinnichenko@lattice-engines.com"
__status__ = "Alpha"

# import modules
from copy import deepcopy
from datetime import datetime
import logging
import os
import time
import traceback

from selenium import webdriver

from TestConfigs import ConfigCSV, ConfigDLC, EtlConfig
from TestRunner import SessionRunner
from Properties import PLSEnvironments


def runLoadGroups(dlc, params, load_groups, max_run_time_in_sec=7200, sleep_time=120):
    command = "Launch Load Group"
    for lg in load_groups:
        params["-g"] = lg
        print "Running %s Load Group" % lg
        status = dlc.runDLCcommand(command, params)
        dlc.getStatus()
        creation_datetime = datetime.now()
        if not status:
            print "Load Group %s failed" % lg
            return False;
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
                return False;
            print "Load Group %s status is %s, will try again in %s seconds" % (lg, lg_status, sleep_time)
            time.sleep(sleep_time)
    return True;

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
        
        
class DLCRunner(SessionRunner):

    def __init__(self, dlc_path=PLSEnvironments.dl_dlc_path, host=PLSEnvironments.pls_test_server, logfile=None, exception=False):
        super(DLCRunner, self).__init__(host, logfile)
        self.exception = exception
        self.ignore = ["command", "definition"]
        self.dlc_path = ""
        if dlc_path is not None:
            self.dlc_path = dlc_path
        self.command = ""
        self.params = {}

    def setDlcPath(self, dlc_path):
        self.dlc_path = dlc_path

    def getDlcPath(self):
        return self.dlc_path

    def getParamsInfo(self, command):
        if command not in ConfigDLC.keys():
            logging.error("No such command [%s] in DLC" % command)
            if self.exception:
                raise "No such command [%s] in DLC" % command
            return None
        required = []
        optional = []
        for param in ConfigDLC[command].keys():
            if param in self.ignore:
                continue
            if ConfigDLC[command][param][0] == "required":
                required.append(param)
            elif ConfigDLC[command][param][0] == "optional":
                optional.append(param)
            else:
                logging.warning("Unknown param [%s] for [%s] command" % (param, command))
        return required, optional

    def validateInput(self, command, params):
        params = deepcopy(params)
        if command not in ConfigDLC.keys():
            logging.error("No such command [%s] in DLC" % command)
            if self.exception:
                raise "No such command [%s] in DLC" % command
            return False
        self.command = ConfigDLC[command]["command"]
        required, optional = self.getParamsInfo(command)
        for param in params.keys():
            if param in required:
                self.params[param] = params[param]
                del params[param]
                required.remove(param)
            elif param in optional:
                self.params[param] = params[param]
                del params[param]
                optional.remove(param)
            else:
                logging.warning("Unknown param [%s] for [%s] command" % (param, command))
                del params[param]
        if len(required) != 0:
            logging.error("Required commands [%s] are missing" % required)
            if self.exception:
                raise "No such command [%s] in DLC" % command
            return False
        else:
            return True

    def constructCommand(self, command, params):
        self.command = ""
        self.params = {}
        if self.validateInput(command, params):
            if self.dlc_path:
                dlc = os.path.join(self.dlc_path, "dlc ")
                # Should be re-worked after DLC becomes platform independent
                dlc = dlc.replace("/", "\\")
            else:
                dlc = "dlc "
            dlc += self.command
            for param in self.params.keys():
                dlc += " %s %s" % (param, self.params[param])
            print dlc
            return dlc
        else:
            return None
        
    def runDLCcommand(self, command, params, local=False):
        cmd = self.constructCommand(command, params)
        if cmd is None:
            logging.error("There is something wrong with your command, please see logs for details")
            if self.exception:
                raise "There is something wrong with your command, please see logs for details"
            return False
        return self.runCommand(cmd, local)

    def testRun(self):
        print "Starting tests. All should be True"
        command = ""
        params = {}
        self.verify(self.validateInput(command, params), False, "1")
        self.verify(self.constructCommand(command, params), None, "2")
        self.verify(self.getParamsInfo(command), None, "3")
        command = "Test Command"
        r, o = self.getParamsInfo(command)
        self.verify(r == ["-u","-s"] and o == ["-p"], True, "4")
        self.verify(self.validateInput(command, params), False, "5")
        self.verify(self.constructCommand(command, params), None, "6")
        r, o = self.getParamsInfo(command)
        params = {"-u":"user", "-s":"http://dataloader"}
        self.verify(r == ["-u","-s"] and o == ["-p"], True, "7")
        self.verify(self.validateInput(command, params), True, "8")
        self.verify(self.constructCommand(command, params), "dlc -Test -u user -s http://dataloader", "9")
        self.setDlcPath("D:\VisiDB")
        self.verify(self.constructCommand(command, params), "D:\VisiDB\dlc -Test -u user -s http://dataloader", "9")
        print "Test status: [%s]" % self.testStatus
        return self.testStatus

class PLSConfigRunner(SessionRunner):
    def __init__(self, pls_url=None, logfile=None, exception=False):
        super(PLSConfigRunner, self).__init__(pls_url, logfile);
        self.exception = exception;
        if pls_url == None:
            self.pls_url = PLSEnvironments.pls_tenant_console_url
        else:
            self.pls_url=pls_url;
        self.plsUI = webdriver.Firefox();
        
    def plsLogin(self):
        self.plsUI.get(self.pls_url);
        time.sleep(30);
        self.plsUI.find_element_by_xpath("//input[@type='text']").clear()
        self.plsUI.find_element_by_xpath("//input[@type='text']").send_keys(PLSEnvironments.pls_tenant_console_user)
        self.plsUI.find_element_by_xpath("//input[@type='password']").clear()
        self.plsUI.find_element_by_xpath("//input[@type='password']").send_keys(PLSEnvironments.pls_tenant_console_pwd)
        self.plsUI.find_element_by_css_selector("button.btn.btn-primary").click()

        time.sleep(30);
        
    def addNewTenant(self,tenantId,CRMTopology):
        self.plsLogin()
        self.plsUI.find_element_by_id("add-new-model-btn").click()
        time.sleep(2);
        self.plsUI.find_element_by_name("tenantId").clear()
        self.plsUI.find_element_by_name("tenantId").send_keys(tenantId)
        time.sleep(1);
        self.plsUI.find_element_by_css_selector("div.modal-footer.ng-scope > button.btn.btn-primary").click()
        time.sleep(5);
        Select(self.plsUI.find_element_by_xpath("//div[2]/select")).select_by_visible_text(CRMTopology)
        Select(self.plsUI.find_element_by_xpath("//div[3]/select")).select_by_visible_text("http://bodcdevvint187.dev.lattice.local:8081")
        self.plsUI.find_element_by_xpath("//input[@type='checkbox']").click()

        self.plsUI.find_element_by_xpath("(//input[@type='text'])[8]").clear()
        self.plsUI.find_element_by_xpath("(//input[@type='text'])[8]").send_keys("smeng@lattice-engines.com")
        self.plsUI.find_element_by_xpath("(//input[@type='text'])[16]").clear()
        self.plsUI.find_element_by_xpath("(//input[@type='text'])[16]").send_keys("10.41.1.247")
        self.plsUI.find_element_by_xpath("//button[2]").click()
        time.sleep(5);
        self.plsUI.find_element_by_css_selector("div.modal-footer.ng-scope > button.btn.btn-primary").click()
        time.sleep(10);
        try: self.assertEqual(tenantId, driver.find_element_by_css_selector("tr.k-alt.ng-scope > td > span.ng-binding").text)
        except AssertionError as e: self.verificationErrors.append(str(e))


                
    def plsSFDCCredentials(self):
        self.plsUI.find_element_by_xpath("//input[@value='']").clear();
        self.plsUI.find_element_by_xpath("//input[@value='']").send_keys(PLSEnvironments.pls_SFDC_user);
        self.plsUI.find_element_by_xpath("(//input[@value=''])[2]").clear();
        self.plsUI.find_element_by_xpath("(//input[@value=''])[2]").send_keys(PLSEnvironments.pls_SFDC_pwd);
        self.plsUI.find_element_by_css_selector("input.js-bard-config-api-input.js-bard-config-api-security-token-input").clear();
        self.plsUI.find_element_by_css_selector("input.js-bard-config-api-input.js-bard-config-api-security-token-input").send_keys(PLSEnvironments.pls_SFDC_key);
        self.plsUI.find_element_by_xpath("//button[@type='button']").click();

        time.sleep(30);        
        assert "(edit)" == self.plsUI.find_element_by_link_text("(edit)").text;
        
    def plsElQCredentials(self):
        self.plsUI.find_element_by_name("apiConfigOptionsMAP").click();
        self.plsUI.find_element_by_xpath("//input[@value='']").clear();
        self.plsUI.find_element_by_xpath("//input[@value='']").send_keys(PLSEnvironments.pls_ELQ_user);
        self.plsUI.find_element_by_xpath("(//input[@value=''])[2]").clear();
        self.plsUI.find_element_by_xpath("(//input[@value=''])[2]").send_keys(PLSEnvironments.pls_ELQ_pwd);
        self.plsUI.find_element_by_css_selector("input.js-bard-config-api-input.js-bard-config-api-security-token-input").clear();
        self.plsUI.find_element_by_css_selector("input.js-bard-config-api-input.js-bard-config-api-security-token-input").send_keys(PLSEnvironments.pls_ELQ_company);
        self.plsUI.find_element_by_xpath("//button[@type='button']").click();
        time.sleep(30);
        assert "(edit)" == self.plsUI.find_element_by_css_selector("a.js-bard-tab-links.js-bard-api-config-edit-link-MAP").text;
        
    def plsMKTOCredentials(self):
        self.plsUI.find_element_by_xpath("(//input[@name='apiConfigOptionsMAP'])[2]").click();
        self.plsUI.find_element_by_xpath("//input[@value='']").clear();
        self.plsUI.find_element_by_xpath("//input[@value='']").send_keys("latticeenginessandbox1_9026948050BD016F376AE6");
        self.plsUI.find_element_by_css_selector("input.js-bard-config-api-input.js-bard-config-api-security-token-input").clear();
        self.plsUI.find_element_by_css_selector("input.js-bard-config-api-input.js-bard-config-api-security-token-input").send_keys("41802295835604145500BBDD0011770133777863CA58");
        self.plsUI.find_element_by_xpath("(//input[@value=''])[2]").clear();
        self.plsUI.find_element_by_xpath("(//input[@value=''])[2]").send_keys("https://na-sj02.marketo.com/soap/mktows/2_0");
        self.plsUI.find_element_by_xpath("//button[@type='button']").click();
        time.sleep(30);
        assert "(edit)" == self.plsUI.find_element_by_css_selector("a.js-bard-tab-links.js-bard-api-config-edit-link-MAP").text;

    def configELQ(self):
        
        self.plsLogin();
        self.plsSFDCCredentials();
        self.plsElQCredentials();
        
        self.plsUI.find_element_by_id("bardConfigNextButton").click();
        self.plsUI.find_element_by_css_selector("input.js-lead-publish-map-field-text").clear();
        self.plsUI.find_element_by_css_selector("input.js-lead-publish-map-field-text").send_keys("C_Lattice_Predictive_Score1");
        self.plsUI.find_element_by_id("bardConfigNextButton").click()  ;      
        time.sleep(10);
        
        self.plsUI.find_element_by_xpath("(//button[@type='button'])[3]").click();
        time.sleep(60);
        
        assert self.plsUI.find_element_by_xpath("//div[10]/div[2]/h3/img").get_attribute("src").endswith("assets/images/logo_eloqua_small.png");

    def configMKTO(self):
        self.plsLogin();
        self.plsSFDCCredentials();
        self.plsMKTOCredentials();
 
        self.plsUI.find_element_by_id("bardConfigNextButton").click();
        self.plsUI.find_element_by_css_selector("input.js-lead-publish-map-field-text").clear();
        self.plsUI.find_element_by_css_selector("input.js-lead-publish-map-field-text").send_keys("latticeforleads__Score__c");
        self.plsUI.find_element_by_id("bardConfigNextButton").click()  ;      
        time.sleep(10);
         
        self.plsUI.find_element_by_xpath("(//button[@type='button'])[3]").click();
        time.sleep(60);

        assert self.plsUI.find_element_by_xpath("//div[10]/div[2]/h3/img").get_attribute("src").endswith("assets/images/logo_marketo_small.png");
 
    def config(self,marketting_app):
        if marketting_app == PLSEnvironments.pls_marketing_app_ELQ:
            self.configELQ();
        else:
            self.configMKTO();
        
        self.plsUI.quit();    

class DLConfigRunner(SessionRunner):  
    def __init__(self, logfile=None, exception=False):
        super(DLConfigRunner, self).__init__(logfile);
        self.exception = exception;
        
    def editDataProviders(self, tenant, dp, connection_string,host=PLSEnvironments.pls_test_server, dlc_path=PLSEnvironments.dl_dlc_path,
                      dl_server=PLSEnvironments.dl_server,
                      user=PLSEnvironments.dl_server_user,
                      password=PLSEnvironments.dl_server_pwd):

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

    def configDLTables(self,tenant,marketting_app):

        self.editDataProviders(tenant, "SQL_PropDataForModeling", PLSEnvironments.SQL_PropDataForModeling);
        self.editDataProviders(tenant, "SQL_PropDataForScoring", PLSEnvironments.SQL_PropDataForScoring);
        self.editDataProviders(tenant, "SQL_PropDataMatch", PLSEnvironments.SQL_PropDataMatch);
        self.editDataProviders(tenant, "SQL_LeadScoring", PLSEnvironments.SQL_LeadScoring);
        self.editDataProviders(tenant, "SQL_DanteDB_DataProvider", PLSEnvironments.SQL_DanteDB_DataProvider);
        self.editDataProviders(tenant, "SQL_LSSBard", PLSEnvironments.SQL_LSSBard);
#         self.editDataProviders(tenant, "SFDC_DataProvider", PLSEnvironments.SFDC_DataProvider);
        
        if PLSEnvironments.pls_marketing_app_ELQ == marketting_app:
            self.editDataProviders(tenant, "SQL_ReportsDB_DataProvider", PLSEnvironments.SQL_ReportsDB_DataProvider_ELQ);
#             self.editDataProviders(tenant, "Eloqua_DataProvider", PLSEnvironments.Eloqua_DataProvider);
        elif PLSEnvironments.pls_marketing_app_MKTO == marketting_app:
            self.editDataProviders(tenant, "SQL_ReportsDB_DataProvider", PLSEnvironments.SQL_ReportsDB_DataProvider_MKTO);  
#             self.editDataProviders(tenant, "Marketo_DataProvider", PLSEnvironments.Marketo_DataProvider);  


    def createMockDataProviders(self, tenant, marketting_app,host=PLSEnvironments.pls_test_server, dlc_path=PLSEnvironments.dl_dlc_path,
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
        if PLSEnvironments.pls_marketing_app_ELQ == marketting_app:
            params["-cs"] = '"%s"' % PLSEnvironments.mock_ELQ_SFDC_DataProvider;
            params["-dpn"] = "mock_SFDC_DataProvider";
        elif PLSEnvironments.pls_marketing_app_MKTO == marketting_app:
            params["-cs"] = '"%s"' % PLSEnvironments.mock_MKTO_SFDC_DataProvider;
            params["-dpn"] = "mock_SFDC_DataProvider";
        elif PLSEnvironments.pls_marketing_app_SFDC == marketting_app:
            params["-cs"] = '"%s"' % PLSEnvironments.mock_SFDC_SFDC_DataProvider;
            params["-dpn"] = "mock_SFDC_DataProvider";
            
        params["-dpt"] = "sqlserver"
        print "Mock SFDC"
        dlc.runDLCcommand(command, params)
        dlc.getStatus()
    
        # Mock Marketting App
        if PLSEnvironments.pls_marketing_app_ELQ == marketting_app:
            params["-cs"] = '"%s"' % PLSEnvironments.mock_ELQ_ELQ_DataProvider;
            params["-dpn"] = "mock_ELQ_ELQ_DataProvider";
        elif PLSEnvironments.pls_marketing_app_MKTO == marketting_app:
            params["-cs"] = '"%s"' % PLSEnvironments.mock_MKTO_MKTO_DataProvider;
            params["-dpn"] = "mock_MKTO_MKTO_DataProvider";
            
        params["-dpt"] = "sqlserver"
        print "Mock %s" % marketting_app
        dlc.runDLCcommand(command, params)
        dlc.getStatus()
    
    def editMockRefreshDataSources(self, tenant, marketting_app,host=PLSEnvironments.pls_test_server, dlc_path=PLSEnvironments.dl_dlc_path,
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
            params["-cn"] = "mock_SFDC_DataProvider"
            print "Updating Refresh Data Source %s for Mock_SFDC_DataProvider" % rds
            dlc.runDLCcommand(command, params)
            #dlc.getStatus()
    
        #LoadMAPDataForModeling
        if marketting_app == "ELQ":
            params["-g"] = "LoadMAPDataForModeling"
            params["-rn"] = "ELQ_Contact"
            params["-cn"] = "mock_ELQ_ELQ_DataProvider";
            print "Updating Refresh Data SourceELQ_Contact for mock_ELQ_ELQ_DataProvider"
            dlc.runDLCcommand(command, params)
            #print dlc.getStatus()
            
    
        #"LoadMAPDataForModeling_ActivityRecord_OtherThanNewLead": "ActivityRecord_OtherThanNewLead",
        elif marketting_app == "MKTO":
            params["-cn"] = "mock_MKTO_MKTO_DataProvider"
            rds_dict = {"LoadMAPDataForModeling_ActivityRecord_NewLead": "ActivityRecord_NewLead",
                        "LoadMAPDataForModeling_LeadRecord": "MKTO_LeadRecord"}
            for lg in rds_dict:
                params["-g"] = lg
                params["-rn"] = rds_dict[lg]
                print "Updating Refresh Data Source %s for Mock_Marketo_Data_Provider" % rds_dict[lg]
                dlc.runDLCcommand(command, params)
                #dlc.getStatus()
                
        elif marketting_app == PLSEnvironments.pls_marketing_app_SFDC:
            params["-g"] = "LoadCRMDataForModeling"
            params["-rn"] = "SFDC_Account"
            params["-cn"] = "mock_SFDC_DataProvider";
            print "Updating Refresh Data Source SFDC_Account for mock_SFDC_DataProvider"
            dlc.runDLCcommand(command, params)
        else:
            print "!!![%s] MARKETTING UP IS NOT SUPPORTED!!!" % marketting_app


    # Step 2.75
    def loadCfgTables(self,tenant,marketting_app, svn_location=PLSEnvironments.svn_location_local, dp_folder=PLSEnvironments.template_location , dlc_host=PLSEnvironments.pls_test_server, dlc_path=PLSEnvironments.dl_dlc_path,
                          local=False, cp=True,
                          dl_server=PLSEnvironments.dl_server,
                          user=PLSEnvironments.dl_server_user,
                          password=PLSEnvironments.dl_server_pwd):
    
        dlc = DLCRunner(host=dlc_host, dlc_path=dlc_path)
        params = {"-s": dl_server,
                  "-u": user,
                  "-p": password,
                  "-t": tenant
                 }
    
        runLoadGroups(dlc, params, ["ImportMetaData"], sleep_time=30)
        dlc.getStatus()
    
        utils = UtilsRunner(host=dlc_host)
        if marketting_app == PLSEnvironments.pls_marketing_app_ELQ:
            marketting = "Eloqua";
        elif marketting_app == PLSEnvironments.pls_marketing_app_MKTO:
            marketting = "Marketo";
        elif marketting_app == PLSEnvironments.pls_marketing_app_SFDC:
            marketting = "Salesforce";
        
        # Pre-process templates
        location = os.path.join(svn_location, "%s csv files for QA to load standard Cfg tables" % marketting)        
        files = utils.getFiles(location, [".csv"])
        schema_map = utils.createSchemaDir(location, files, marketting)
        reloc_dir = utils.relocateCsvFile(dp_folder, schema_map, marketting, local, cp)

        # Create New Load Group
        print "Creating New Load Group"
        lg_name = "Group_LoadCfgTables_%s" % marketting
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
            dp_params["-dpn"] = "%s_%s_DataProvider" % (marketting, schema)
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
            data_provider = "%s_%s_DataProvider" % (marketting, schema)
            rd_params["-rn"] = "%s_RDS" % data_provider
            rd_params["-sn"] = schema
            rd_params["-cn"] = data_provider
            #dlc.constructCommand(command, rd_params)
            dlc.runDLCcommand(command, rd_params)
            dlc.getStatus()
    
        runLoadGroups(dlc, params, [lg_name],sleep_time=120)

class DanteRunner(SessionRunner):
    def __init__(self, SFDC_url=None, logfile=None, exception=False):
        super(DanteRunner, self).__init__(SFDC_url, logfile);
        self.exception = exception;
        if SFDC_url == None:
            self.sfdc_url=PLSEnvironments.pls_SFDC_login_url;
        else:
            self.sfdc_url=SFDC_url;
        self.sfdcUI = None
     
    def SFDCLogin(self): 
        self.sfdcUI = webdriver.Firefox();
        self.sfdcUI.get(self.sfdc_url);
        time.sleep(15);       
        self.sfdcUI.find_element_by_id("username").clear()
        self.sfdcUI.find_element_by_id("username").send_keys(PLSEnvironments.pls_SFDC_user)
        self.sfdcUI.find_element_by_id("password").clear()
        self.sfdcUI.find_element_by_id("password").send_keys(PLSEnvironments.pls_SFDC_pwd)
        self.sfdcUI.find_element_by_id("Login").click()
        time.sleep(30);
     
    def setDanteConfigSettings(self):
        self.sfdcUI.find_element_by_id("userNav-arrow").click()
        self.sfdcUI.find_element_by_link_text("Setup").click()
        time.sleep(5);
        self.sfdcUI.find_element_by_css_selector("#DevToolsIntegrate_icon > img.setupImage").click()
        time.sleep(5);
        self.sfdcUI.find_element_by_id("CustomSettings_font").click()
        time.sleep(5);
        self.sfdcUI.find_element_by_xpath("//a[contains(@href, '/a06/o')]").click()
        time.sleep(20);
        dt_url = "https://%s/DT_%s" % (PLSEnvironments.pls_server,PLSEnvironments.pls_bard_1[3:])
        self.sfdcUI.find_element_by_id("CS_list:CS_Form:theDetailPageBlock:thePageBlockButtons:edit").click()
        time.sleep(10);
        self.sfdcUI.find_element_by_id("CS_Edit:CS_Form:thePageBlock:thePageBlockSection:latticeforleads__url__c").clear()
        self.sfdcUI.find_element_by_id("CS_Edit:CS_Form:thePageBlock:thePageBlockSection:latticeforleads__url__c").send_keys(dt_url)
        self.sfdcUI.find_element_by_id("CS_Edit:CS_Form:thePageBlock:thePageBlockButtons:save").click()
 
    def checkDanteValueFromUI(self,dante_lead):
        self.SFDCLogin()
        self.setDanteConfigSettings()
        lead_url = "%s%s" % (PLSEnvironments.pls_SFDC_url[0:PLSEnvironments.pls_SFDC_url.find("services")],dante_lead)
        time.sleep(10);
        print "the lead which you want to check is: %s" % lead_url
        self.sfdcUI.get(lead_url)
        
    def checkDanteValueFromDB(self,dante_lead):
        connection_string = PLSEnvironments.SQL_conn_dante;
        query = "SELECT count(*)  FROM [LeadCache] where [Salesforce_ID]='%s' " % dante_lead; 
        result = self.getQuery(connection_string, query);
        assert result[0][0]==1
    def checkDanteValue(self,dante_lead):
        checkDanteValueFromDB(dante_lead)
                
class UtilsRunner(SessionRunner):

    def __init__(self, host=PLSEnvironments.pls_test_server, logfile=None, exception=False):
        super(UtilsRunner, self).__init__(host, logfile)
        self.exception = exception

    def createSchemaDir(self, original_location, file_list, tag=""):
        schema_map = {}
        if original_location.startswith("~"):
            original_location = os.path.expanduser(original_location)
        file_map = ConfigCSV[tag]
        for filename in file_list:
            print filename;
            if filename not in file_map:
                print "No known schema for %s" % filename
                continue
            if file_map[filename] in schema_map:
                schema_map[file_map[filename]].append(os.path.join(original_location, filename))
            else:
                schema_map[file_map[filename]] = [os.path.join(original_location, filename)]
                
            print schema_map[file_map[filename]];
            
        return schema_map

    def relocateCsvFile(self, new_location, schema_map, tag, local=False, cp=True):
        relocation_map = {}
        for schema in schema_map:
            new_directory = os.path.join(new_location, "%s_%s" % (tag, schema))
            relocation_map[schema] = new_directory
            for fname in schema_map[schema]:
                if cp:
                    print "copying %s to %s" % (fname, new_directory)
                    self.cpFile(fname, new_directory, local)
        return relocation_map


class JamsRunner(SessionRunner):
    def __init__(self, jams_conn=PLSEnvironments.SQL_JAMS_CFG, logfile=None, exception=False):
        super(JamsRunner, self).__init__(logfile)
        self.exception = exception
        self.connection_string = jams_conn;

    def setJamsTenant(self,bard_name,queue_name=None):
        # Wait for the leads to be scored
        dlc = SessionRunner()
        if queue_name == None:
            queue = PLSEnvironments.pls_server[0:PLSEnvironments.pls_server.find(".")];  # @UndefinedVariable
        else:
            queue=queue_name;
        query="exec AlterJAMSDanteCfg '%s', '%s'" % (bard_name,queue);
#         print self.connection_string;
#         print query;
        results = dlc.execProc(self.connection_string, query);
        print results;
        print query;
        print self.connection_string;
        return results[0][0];
    def setJamsTenantCycles(self,bard_name,queue_name=None,cycle_times=3):
        wait_cycle = 0
        while(wait_cycle < cycle_times):
            result = self.setJamsTenant(bard_name);
            if 1 == result:
                print "==>Jams set up successfully!";
                return True;
            print "Sleeping for 600 seconds";
            wait_cycle += 1
            time.sleep(600)
        print "==>Jams set up failedd!";
        return False;
    
def main():
    #basePretzelTest()
    DLCRunner().testRun()


if __name__ == '__main__':
    main()