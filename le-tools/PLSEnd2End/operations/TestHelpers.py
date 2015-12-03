#!/usr/local/bin/python
# coding: utf-8

# Base test framework test helpers
import re

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
import requests
import json

from selenium import webdriver

from TestConfigs import ConfigCSV, ConfigDLC, EtlConfig
from TestRunner import SessionRunner
from Properties import PLSEnvironments


def runLoadGroups(tenant, load_groups, max_run_time_in_sec=10800, sleep_time=30):
    for lg in load_groups:
        succeed, launchid = runLoadGroup(tenant, lg, max_run_time_in_sec, sleep_time)
        if not succeed:
            break;
    return succeed


def runLoadGroup(tenant, load_group, max_run_time_in_sec=7200, sleep_time=30):
    command = "Launch Load Group"
    params = {"-s": PLSEnvironments.dl_server,
              "-u": PLSEnvironments.dl_server_user,
              "-p": PLSEnvironments.dl_server_pwd,
              "-t": tenant,
              "-g": load_group
              }
    print "Start to Run Load Group: %s" % load_group
    dlc = DLCRunner(PLSEnvironments.dlc_path)
    status = dlc.runDLCcommand(command, params)
    if not status:
        print "Try run Load Group \"%s\" failed" % load_group
        return False, None
    launchid = dlc.stdout.split(':', 1)[1].strip().strip('.')

    start = time.time()
    lg_start_datetime = None
    lg_last_succeed_date = None
    lg_last_fail_date = None
    lg_succeed = True
    while (True):
        # If overtime,then break
        if (time.time() - start) >= max_run_time_in_sec:
            print "Load Group %s start at %s and did not succeed in %s seconds" % (
                load_group, datetime.fromtimestamp(start), max_run_time_in_sec)
            lg_succeed = False
            break

        returnDict = getLoadGroupStatus(dlc, tenant, load_group)
        if returnDict["Start"].strip() != "-":
            lg_start_datetime = datetime.strptime(returnDict["Start"], "%Y-%m-%d %H:%M:%S")
        if returnDict["Last Succeeded"] != "Never":
            lg_last_succeed_date = datetime.strptime(returnDict["Last Succeeded"], "%Y-%m-%d %H:%M:%S")
        if returnDict["Last Failed"] != "Never":
            lg_last_fail_date = datetime.strptime(returnDict["Last Failed"], "%Y-%m-%d %H:%M:%S")
        lg_status = returnDict["State"]
        if lg_status == "Launch Succeeded":
            print "Load Group %s Launch Succeeded" % load_group
            break
        if lg_status == "Idle":
            # Typicaly, judge the start time and finish time first
            if lg_start_datetime:
                if lg_last_succeed_date and lg_last_succeed_date > lg_start_datetime:
                    print "Load Group %s Launch Succeeded" % load_group
                    break
                if lg_last_fail_date and lg_last_fail_date > lg_start_datetime:
                    print "Load Group %s run failed" % load_group
                    lg_succeed = False
                    break
                else:
                    print "Load Group %s Launch Succeeded" % load_group
                    break
            else:  # When didn't get the start time, judge the Last failed time
                if not lg_last_fail_date:  # Last Failed is Never
                    print "Load Group %s Launch Succeeded" % load_group
                    break
                else:  # Last Failed is not Never
                    if not lg_last_succeed_date:  # Last Succeeded is Never
                        print "Load Group %s run failed" % load_group
                        lg_succeed = False
                        break
                    else:  # Both Last Succeeded and Last Failed have value
                        if lg_last_fail_date > lg_last_succeed_date:
                            print "Load Group %s run failed" % load_group
                            lg_succeed = False
                            break
                        else:
                            print "Load Group %s Launch Succeeded" % load_group
                            break
        print "Load Group %s status is: %s, will try again in %s seconds" % (load_group, lg_status, sleep_time)
        time.sleep(sleep_time)
    print "Run load group %s cost: %s s" % (load_group, time.time() - start)
    return lg_succeed, launchid


def getLoadGroupStatus(dlc, tenant, load_group):
    command = "Get Load Group Status"
    params = {"-s": PLSEnvironments.dl_server,
              "-u": PLSEnvironments.dl_server_user,
              "-p": PLSEnvironments.dl_server_pwd,
              "-t": tenant,
              "-g": load_group
              }
    print "Get status of load group %s command return: %s" % (load_group, dlc.runDLCcommand(command, params))
    returnTxt = dlc.stdout
    # print returnTxt

    returnDict = {}
    for line in returnTxt.split("\n"):
        if line.find(":") > 0:
            kv = line.split(':', 1)
            print kv
            returnDict[kv[0]] = kv[1].strip()

    return returnDict


class DLCRunner(SessionRunner):
    def __init__(self, dlc_path=PLSEnvironments.dlc_path, logfile=None, exception=False):
        super(DLCRunner, self).__init__(logfile)
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

    def runDLCcommand(self, command, params, local=True):
        cmd = self.constructCommand(command, params)
        if cmd is None:
            logging.error("There is something wrong with your command, please see logs for details")
            if self.exception:
                raise "There is something wrong with your command, please see logs for details"
            return False
        return self.runCommand(cmd, local)


class LPConfigRunner(SessionRunner):
    def __init__(self, pls_tenant_console_url=None, model_url=None, logfile=None, exception=False):
        super(LPConfigRunner, self).__init__(logfile);
        self.exception = exception;
        if pls_tenant_console_url == None:
            self.pls_tenant_console_url = PLSEnvironments.pls_tenant_console_url;
        else:
            self.pls_tenant_console_url = pls_tenant_console_url;
        if model_url == None:
            self.model_url = PLSEnvironments.pls_model_url;
        else:
            self.model_url = model_url;

    def tenantConsoleLogin(self):
        url = self.pls_tenant_console_url + "/admin/adlogin"
        header = {"Content-Type": "application/json"};
        body = {"Username": PLSEnvironments.pls_tenant_console_user,
                "Password": PLSEnvironments.pls_tenant_console_pwd};
        response = requests.post(url, headers=header, data=json.dumps(body));

        if response:
            results = json.loads(response.text);
            access_token = results["Token"]
            return access_token;
        else:
            return None;

    def modelLogin(self):
        url = self.model_url + "/pls/login"
        header = {"Content-Type": "application/json"};
        body = {"Username": PLSEnvironments.pls_model_user, "Password": PLSEnvironments.pls_model_pwd_encryptd};
        response = requests.post(url, headers=header, data=json.dumps(body));

        if response:
            results = json.loads(response.text);
            return "%s.%s" % (results["Uniqueness"], results["Randomness"]);
        else:
            return None;

    def modelLoginAttach(self, tenantName, authorization):
        url = self.model_url + "/pls/attach"
        header = {"Content-Type": "application/json", "Authorization": authorization};
        body = {"DisplayName": tenantName, "Identifier": "%s.%s.Production" % (tenantName, tenantName)};
        response = requests.post(url, headers=header, data=json.dumps(body));

    def addNewTenant(self, tenantName, topology, jamsAgent, templateVersion, visidbServer, dlServer=None):
        url = self.pls_tenant_console_url + "/admin/tenants/%s?contractId=%s" % (tenantName, tenantName);
        header = {"Content-Type": "application/json", "Authorization": self.tenantConsoleLogin()};
        response = requests.post(url, headers=header, data=json.dumps(
            self.newTenantBody(tenantName, topology, jamsAgent, templateVersion, visidbServer, dlServer)));

        if response.status_code == 200:
            return True
        else:
            print response.text
            return False

    def lpSFDCCredentials(self, tenantName):
        authorization = self.modelLogin();
        self.modelLoginAttach(tenantName, authorization);

        url = self.model_url + "/pls/credentials/sfdc/?tenantId=%s.%s.Production&isProduction=true" % (
            tenantName, tenantName)
        header = {"Content-Type": "application/json", "Authorization": authorization};
        body = {"Company": "null", "OrgId": "null", "Url": "null", "UserName": PLSEnvironments.pls_SFDC_user,
                "Password": PLSEnvironments.pls_SFDC_pwd, "SecurityToken": PLSEnvironments.pls_SFDC_key}
        response = requests.post(url, headers=header, data=json.dumps(body));

        if response.status_code == 200:
            return True
        else:
            return False

    def lpElQCredentials(self, tenantName):
        authorization = self.modelLogin();
        self.modelLoginAttach(tenantName, authorization);

        url = self.model_url + "/pls/credentials/eloqua/?tenantId=%s.%s.Production&isProduction=true" % (
            tenantName, tenantName)
        header = {"Content-Type": "application/json", "Authorization": authorization};
        body = {"Company": PLSEnvironments.pls_ELQ_company, "OrgId": "null", "Url": PLSEnvironments.pls_ELQ_url,
                "UserName": PLSEnvironments.pls_ELQ_user, "Password": PLSEnvironments.pls_ELQ_pwd,
                "SecurityToken": "null"}
        response = requests.post(url, headers=header, data=json.dumps(body));

        if response.status_code == 200:
            return True
        else:
            return False

    def lpMKTOCredentials(self, tenantName):
        authorization = self.modelLogin();
        self.modelLoginAttach(tenantName, authorization);

        url = self.model_url + "/pls/credentials/marketo/?tenantId=%s.%s.Production&isProduction=true" % (
            tenantName, tenantName)
        header = {"Content-Type": "application/json", "Authorization": authorization};
        body = {"Company": "", "OrgId": "null", "Url": PLSEnvironments.pls_MKTO_url,
                "UserName": PLSEnvironments.pls_MKTO_Username, "Password": PLSEnvironments.pls_MKTO_Password,
                "SecurityToken": "null"}
        response = requests.post(url, headers=header, data=json.dumps(body));

        if response.status_code == 200:
            return True
        else:
            return False

    def init(self, tenant, marketting_app):
        ''' configure dataLoader settings '''
        print "add new tenant: %s via tenant console" % tenant
        created=False;
        if marketting_app == PLSEnvironments.pls_marketing_app_ELQ:
            if True == self.addNewTenant(tenant, "Eloqua", PLSEnvironments.jams_server, PLSEnvironments.pls_version,
                                         PLSEnvironments.dl_server_name):
                print "Credentials will be input after 5 minutes."
                time.sleep(300);
                self.lpSFDCCredentials(tenant)
                self.lpElQCredentials(tenant);
                created=True;
        elif marketting_app == PLSEnvironments.pls_marketing_app_MKTO:
            if True == self.addNewTenant(tenant, "Marketo", PLSEnvironments.jams_server, PLSEnvironments.pls_version,
                                         PLSEnvironments.dl_server_name):
                print "Credentials will be input after 5 minutes."
                time.sleep(300);
                self.lpSFDCCredentials(tenant)
                self.lpMKTOCredentials(tenant)
                created=True;
        else:
            if True == self.addNewTenant(tenant, "SFDC", PLSEnvironments.jams_server, PLSEnvironments.pls_version,
                                         PLSEnvironments.dl_server_name):
                print "Credentials will be input after 5 minutes."
                time.sleep(300);
                self.lpSFDCCredentials(tenant)
                created=True;
        print "configure dataloader settings"
        if created:
            dlConfig = DLConfigRunner();
            dlConfig.configDLTables(tenant, marketting_app);
            dlConfig.createMockDataProviders(tenant, marketting_app);
            dlConfig.editMockRefreshDataSources(tenant, marketting_app);
            dlConfig.loadCfgTables(tenant);
            jamsRunner = JamsRunner()
            jamsRunner.setJamsTenant(tenant)
        else:
            print "Failed to add new tenant: %s via tenant console" % tenant
            assert False

    def lpGetModel(self, authorization):
        url = self.model_url + "/pls/modelsummaries/"
        header = {"Content-Type": "application/json", "Authorization": authorization};
        response = requests.get(url, headers=header);

        results = json.loads(response.text);
        return results

    def lpActivateModel(self, tenantName, modelPriority=0):
        authorization = self.modelLogin();
        self.modelLoginAttach(tenantName, authorization);
        count=0
        while(count<120):
            models = self.lpGetModel(authorization);

            if len(models) >= modelPriority:
                break;
            print "we can't get the model, will try again after 30 seconds."
            count=count+1
            time.sleep(120)
        if count==30:
            print "The existing models: %d is less than the expected: %d" % (len(models), modelPriority + 1)
            return False;

        url = self.model_url + "/pls/segments/list"
        header = {"Content-Type": "application/json", "Authorization": authorization};
        body = [{"ModelId": models[modelPriority]["Id"], "ModelName": models[modelPriority]["Name"],
                 "Name": "LATTICE_DEFAULT_SEGMENT", "NewModelId": models[modelPriority]["Id"], "Priority": "2"}]
        response = requests.post(url, headers=header, data=json.dumps(body));

        return json.loads(response.text)["Success"]

    def newTenantBody(self, tenantName, topology, jamsAgent, templateVersion, visidbServer, dlServer=None):
        if dlServer == None:
            dlServer = visidbServer;
        SpaceConfig = {}
        ContractInfo = {}
        TenantInfo = {}
        CustomerSpaceInfo = {}
        ConfigDirectories = []

        SpaceConfig["Product"] = "Lead Prioritization";
        SpaceConfig["Products"] = ["Lead Prioritization"];
        SpaceConfig["Topology"] = topology;
        SpaceConfig["DL_Address"] = "http://%s.dev.lattice.local:8081" % dlServer;

        ContractInfo["properties"] = {};

        TenantInfo["properties"] = {"displayName": tenantName,
                                    "description": "A LPA tenant under the contract %s" % tenantName};

        CustomerSpaceInfo["properties"] = {"displayName": tenantName,
                                           "description": "A LPA solution for %s in %s" % (tenantName, tenantName)};
        CustomerSpaceInfo["featureFlags"] = "{\"Dante\": true}";

        ConfigDirectories.append({"RootPath": "/DLTemplate", "Nodes": []});
        ConfigDirectories.append({"RootPath": "/Dante", "Nodes": []})
        ConfigDirectories.append({"RootPath": "/PLS",
                                  "Nodes": [{"Node": "ExternalAdminEmails", "Data": "[]"},
                                            {"Node": "LatticeAdminEmails", "Data": "[]"},
                                            {"Node": "SuperAdminEmails",
                                             "Data": "[\"bnguyen@lattice-engines.com\",\"smeng@lattice-engines.com\"]"}]})
        ConfigDirectories.append({"RootPath": "/VisiDBTemplate", "Nodes": []})

        BardJams = {}
        BardJams["RootPath"] = "/BardJams";
        Nodes = []
        Nodes.append({"Node": "Active", "Data": "1"})
        Nodes.append({"Node": "Agent_Name", "Data": jamsAgent})
        Nodes.append({"Node": "DL_Password", "Data": "adm1nDLpr0d"})
        Nodes.append({"Node": "DL_TenantName", "Data": tenantName})
        Nodes.append({"Node": "DL_URL", "Data": "http://%s.dev.lattice.local:8081" % dlServer})
        Nodes.append({"Node": "DL_User", "Data": "admin.dataloader@lattice-engines.com"})
        Nodes.append({"Node": "DanteManifestPath",
                      "Data": "\\\\%s\\DataLoader\\Customers\\%s\\Launch" % (visidbServer, tenantName)})
        Nodes.append({"Node": "DanteTool_Path", "Data": "C:\\Lattice\\Dante3\\Tools\\bin\\"})
        Nodes.append({"Node": "Dante_Queue_Name", "Data": ""})
        Nodes.append({"Node": "DataLoaderTools_Path", "Data": "C:\\LEO\\DLTools\\"})
        Nodes.append({"Node": "Data_ArchivePath",
                      "Data": "\\\\%s\\DataLoader\\Customers\\%s\\\\Backup" % (visidbServer, tenantName)})
        Nodes.append({"Node": "Data_LaunchPath",
                      "Data": "\\\\%s\\DataLoader\\Customers\\%s\\Launch" % (visidbServer, tenantName)})
        Nodes.append({"Node": "ImmediateFolderStruct", "Data": "LEO\\DataLoader\\DLPROD\\ImmediateJobs"})
        Nodes.append({"Node": "JAMSUser", "Data": "PROD\\s-jams"})
        Nodes.append({"Node": "NotificationEmail", "Data": "platformoperations@lattice-engines.com"})
        Nodes.append({"Node": "NotifyEmailJob", "Data": "LEO\\DataLoader\\DLOmnibus\\DL_NOTIFY_EMAIL"})
        Nodes.append({"Node": "Queue_Name", "Data": ""})
        Nodes.append({"Node": "ScheduledFolderStruct", "Data": "LEO\\DataLoader\\DLPROD\\ScheduledJobs"})
        Nodes.append({"Node": "TenantType", "Data": "P"})
        Nodes.append({"Node": "WeekdaySchedule_Name", "Data": "All_Weekday"})
        Nodes.append({"Node": "WeekendSchedule_Name", "Data": "All_Weekend"})
        BardJams["Nodes"] = Nodes;
        ConfigDirectories.append(BardJams);

        VisiDBDL = {}
        VisiDBDL["RootPath"] = "/VisiDBDL";
        VisiDBDL["Nodes"] = [
            {"Node": "DL", "Children": [{"Node": "DataStore", "Data": "\\\\%s\\DataLoader\\Customers" % visidbServer},
                                        {"Node": "DataStore_Backup",
                                         "Data": "\\\\%s\\DataLoader\\Customers\\%s\\Backup" % (
                                             visidbServer, tenantName)},
                                        {"Node": "DataStore_Launch",
                                         "Data": "\\\\%s\\DataLoader\\Customers\\%s\\Launch" % (
                                             visidbServer, tenantName)},
                                        {"Node": "DataStore_Status",
                                         "Data": "\\\\%s\\DataLoader\\Customers\\%s\\Status" % (
                                             visidbServer, tenantName)},
                                        {"Node": "OwnerEmail", "Data": "richard.liu@lattice-engines.com"}]},
            {"Node": "TemplateVersion", "Data": templateVersion},
            {"Node": "TenantAlias", "Data": ""},
            {"Node": "VisiDB", "Children": [{"Node": "CacheLimit", "Data": "8192"},
                                            {"Node": "CaseSensitive", "Data": "false"},
                                            {"Node": "CreateNewVisiDB", "Data": "true"},
                                            {"Node": "DiskspaceLimit", "Data": "0"},
                                            {"Node": "PermStoreFullPath",
                                             "Data": "\\\\%s\\VisiDB\\PermanentStore\\%s\\%s" % (
                                                 visidbServer, visidbServer, tenantName)},
                                            {"Node": "PermanentStore",
                                             "Data": "\\\\%s\\VisiDB\\PermanentStore" % visidbServer},
                                            {"Node": "PermanentStoreOption", "Data": "Master"},
                                            {"Node": "ServerName", "Data": visidbServer},
                                            {"Node": "VisiDBFileDirectory", "Data": ""},
                                            {"Node": "VisiDBName", "Data": ""}]
             }];
        ConfigDirectories.append(VisiDBDL);

        tenantBody = {}
        tenantBody["SpaceConfig"] = SpaceConfig;
        tenantBody["ContractInfo"] = ContractInfo;
        tenantBody["TenantInfo"] = TenantInfo;
        tenantBody["CustomerSpaceInfo"] = CustomerSpaceInfo;
        tenantBody["ConfigDirectories"] = ConfigDirectories;

        return tenantBody;


class DLConfigRunner(SessionRunner):
    def __init__(self, logfile=None, exception=False):
        super(DLConfigRunner, self).__init__(logfile);
        self.exception = exception;

    def editDataProviders(self, tenant, dp, connection_string, host=None, dlc_path=PLSEnvironments.dlc_path,
                          dl_server=PLSEnvironments.dl_server,
                          user=PLSEnvironments.dl_server_user,
                          password=PLSEnvironments.dl_server_pwd):

        dlc = DLCRunner(dlc_path=dlc_path)
        command = "Edit Data Provider"
        params = {"-s": dl_server,
                  "-u": user,
                  "-p": password,
                  "-t": tenant,
                  "-v": "true"
                  }
        params["-cs"] = '"%s"' % connection_string
        params["-dpn"] = dp
        # print dp
        dlc.runDLCcommand(command, params)
        dlc.getStatus()

    def configDLTables(self, tenant, marketting_app):

        self.editDataProviders(tenant, "SQL_PropDataForModeling", PLSEnvironments.SQL_PropDataForModeling);
        self.editDataProviders(tenant, "SQL_PropDataForScoring", PLSEnvironments.SQL_PropDataForScoring);
        self.editDataProviders(tenant, "SQL_PropDataMatch", PLSEnvironments.SQL_PropDataMatch);
        self.editDataProviders(tenant, "SQL_LeadScoring", PLSEnvironments.SQL_LeadScoring);
        self.editDataProviders(tenant, "SQL_MultiTenant", PLSEnvironments.SQL_MultiTenant);
        self.editDataProviders(tenant, "SQL_DanteDB_DataProvider", PLSEnvironments.SQL_DanteDB_DataProvider);
        self.editDataProviders(tenant, "SQL_LSSBard", PLSEnvironments.SQL_LSSBard);

    def createMockDataProviders(self, tenant, marketting_app, dlc_path=PLSEnvironments.dlc_path,
                                dl_server=PLSEnvironments.dl_server,
                                user=PLSEnvironments.dl_server_user,
                                password=PLSEnvironments.dl_server_pwd):

        dlc = DLCRunner(dlc_path=dlc_path)
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

    def editMockRefreshDataSources(self, tenant, marketting_app, dlc_path=PLSEnvironments.dlc_path,
                                   dl_server=PLSEnvironments.dl_server,
                                   user=PLSEnvironments.dl_server_user,
                                   password=PLSEnvironments.dl_server_pwd):

        dlc = DLCRunner(dlc_path=dlc_path)
        command = "Edit Refresh Data Source"
        params = {"-s": dl_server,
                  "-u": user,
                  "-p": password,
                  "-t": tenant,
                  "-f": "@recordcount(2000000)"
                  }

        # LoadCRMDataForModeling
        rds_list = ["SFDC_User", "SFDC_Contact", "SFDC_Lead", "SFDC_Opportunity", "SFDC_OpportunityContactRole"]
        for rds in rds_list:
            params["-g"] = "LoadCRMDataForModeling"
            params["-rn"] = rds
            params["-cn"] = "mock_SFDC_DataProvider"
            print "Updating Refresh Data Source %s for Mock_SFDC_DataProvider" % rds
            dlc.runDLCcommand(command, params)
            # dlc.getStatus()

        # LoadMAPDataForModeling
        if marketting_app == "ELQ":
            params["-g"] = "LoadMAPDataForModeling"
            params["-rn"] = "ELQ_Contact"
            params["-cn"] = "mock_ELQ_ELQ_DataProvider";
            print "Updating Refresh Data SourceELQ_Contact for mock_ELQ_ELQ_DataProvider"
            dlc.runDLCcommand(command, params)
            # print dlc.getStatus()


        # "LoadMAPDataForModeling_ActivityRecord_OtherThanNewLead": "ActivityRecord_OtherThanNewLead",
        elif marketting_app == "MKTO":
            params["-cn"] = "mock_MKTO_MKTO_DataProvider"
            rds_dict = {"LoadMAPDataForModeling_ActivityRecord_NewLead": "ActivityRecord_NewLead",
                        "LoadMAPDataForModeling_LeadRecord": "MKTO_LeadRecord"}
            for lg in rds_dict:
                params["-g"] = lg
                params["-rn"] = rds_dict[lg]
                print "Updating Refresh Data Source %s for Mock_Marketo_Data_Provider" % rds_dict[lg]
                dlc.runDLCcommand(command, params)
                # dlc.getStatus()

        elif marketting_app == PLSEnvironments.pls_marketing_app_SFDC:
            params["-g"] = "LoadCRMDataForModeling"
            params["-rn"] = "SFDC_Account"
            params["-cn"] = "mock_SFDC_DataProvider";
            print "Updating Refresh Data Source SFDC_Account for mock_SFDC_DataProvider"
            dlc.runDLCcommand(command, params)
        else:
            print "!!![%s] MARKETTING UP IS NOT SUPPORTED!!!" % marketting_app

    # Step 2.75
    def loadCfgTables(self, tenant, dlc_path=PLSEnvironments.dlc_path,
                      local=False, cp=True,
                      dl_server=PLSEnvironments.dl_server,
                      user=PLSEnvironments.dl_server_user,
                      password=PLSEnvironments.dl_server_pwd):

        dlc = DLCRunner(dlc_path=dlc_path)
        params = {"-s": dl_server,
                  "-u": user,
                  "-p": password,
                  "-t": tenant
                  }

        runLoadGroup(tenant, "ImportMetaData", sleep_time=30)

        runLoadGroup(tenant, "ImportCfgTables", sleep_time=30)


class DanteRunner(SessionRunner):
    def __init__(self, SFDC_url=None, logfile=None, exception=False):
        super(DanteRunner, self).__init__(logfile);
        self.exception = exception;
        if SFDC_url == None:
            self.sfdc_url = PLSEnvironments.pls_SFDC_login_url;
        else:
            self.sfdc_url = SFDC_url;
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
        dt_url = "https://%s/DT_%s" % (PLSEnvironments.pls_server, PLSEnvironments.pls_bard_1[3:])
        self.sfdcUI.find_element_by_id("CS_list:CS_Form:theDetailPageBlock:thePageBlockButtons:edit").click()
        time.sleep(10);
        self.sfdcUI.find_element_by_id(
            "CS_Edit:CS_Form:thePageBlock:thePageBlockSection:latticeforleads__url__c").clear()
        self.sfdcUI.find_element_by_id(
            "CS_Edit:CS_Form:thePageBlock:thePageBlockSection:latticeforleads__url__c").send_keys(dt_url)
        self.sfdcUI.find_element_by_id("CS_Edit:CS_Form:thePageBlock:thePageBlockButtons:save").click()

    def checkDanteValueFromUI(self, dante_lead):
        self.SFDCLogin()
        self.setDanteConfigSettings()
        lead_url = "%s%s" % (PLSEnvironments.pls_SFDC_url[0:PLSEnvironments.pls_SFDC_url.find("services")], dante_lead)
        time.sleep(10);
        print "the lead which you want to check is: %s" % lead_url
        self.sfdcUI.get(lead_url)

    def checkDanteValueFromDB(self, tenantName, dante_lead):
        connection_string = PLSEnvironments.SQL_conn_dante;
        query = "SELECT count(*)  FROM [LeadCache] where [Customer_ID]='%s' and [Salesforce_ID]='%s' " % (tenantName, dante_lead);
        print query
        result = self.getQuery(connection_string, query);
        assert result[0][0] == 1,result[0][0]

    def checkDanteValues(self, dante_leads):
        for danteLead in dante_leads:
            self.checkDanteValue(danteLead.values()[0])

    def checkDanteValue(self,tenantName, dante_lead):
        self.checkDanteValueFromDB(tenantName, dante_lead)


class UtilsRunner(SessionRunner):
    def __init__(self, logfile=None, exception=False):
        super(UtilsRunner, self).__init__(logfile)
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

    def setJamsTenant(self, tenant_name, queue_name=None, dante_db=PLSEnvironments.dante_server_db):
        runner = SessionRunner()
        if not queue_name:
            queue_name = 'QATest' + re.search('\\d+$', PLSEnvironments.dante_server_name).group(0)
        query = "exec AlterJAMSDanteCfg @Tenant='%s', @Queue='%s', @DanteDB='%s'" % (
        tenant_name, queue_name, dante_db.split('_')[1]);
        #       print self.connection_string;
        #       print query;
        results = runner.execProc(self.connection_string, query);
        print "exec AlterJAMSDanteCfg: ", "Succeed" if results[0][0] == 1 else "Failed";
        return results[0][0];

    def setJamsTenant_TryMultiTimes(self, tenant_name, queue_name, dante_db, cycle_times=3):
        wait_cycle = 0
        while (wait_cycle < cycle_times):
            result = self.setJamsTenant(tenant_name, queue_name, dante_db);
            if 1 == result:
                print "==>Jams set up successfully!";
                return True;
            print "Sleeping for 600 seconds";
            wait_cycle += 1
            time.sleep(600)
        print "==>Jams set up failedd!";
        return False;


def main():
    # basePretzelTest()
    DLCRunner().testRun()


if __name__ == '__main__':
    main()
