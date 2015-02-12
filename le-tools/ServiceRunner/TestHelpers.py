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
import logging
import os
import traceback
from copy import deepcopy
from datetime import datetime
from TestConfigs import ConfigCSV
from TestConfigs import ConfigDLC
from TestConfigs import EtlConfig
from TestRunner import SessionRunner

class DLCRunner(SessionRunner):

    def __init__(self, dlc_path=None, host="http://localhost:5000", logfile=None, exception=False):
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

class PretzelRunner(SessionRunner):

    def __init__(self, svn_location, build_path, specs_path,
                 host="http://localhost:5000",
                 logfile=None, exception=False):
        super(PretzelRunner, self).__init__(host, logfile)
        self.exception = exception
        self.topologies = ["EloquaAndSalesforceToEloqua",
                           "MarketoAndSalesforceToMarketo",
                           "EloquaAndSalesforceToSalesforce",
                           "SalesforceAndEloquaToEloqua",
                           "SalesforceAndEloquaToSalesforce",
                           "MarketoAndSalesforceToSalesforce",
                           "SalesforceAndMarketoToMarketo",
                           "SalesforceAndMarketoToSalesforce",
                           "SalesforceToSalesforce"]
        self.svn_location = os.path.expanduser(svn_location)
        self.build_path = os.path.expanduser(build_path)
        self.specs_path = os.path.expanduser(specs_path)
        self.revision = 1
        self.pretzel_tool = os.path.join(self.build_path,"Pretzel", "Install", "bin", "PretzelAdminTool.exe")
        self.main_py = os.path.join(self.svn_location, "Python script", "main.py")
        self.build_location = os.path.join(self.build_path, "Pretzel", "PretzelDaemon", "bin", "scripts", "production")

    def isValidTopology(self, topology):
        if topology in self.topologies:
            return True
        return False

    def getMain(self, localCopy=False):
        # SVN can be both on Linux and Windows, so this way:
        main_py = self.main_py
        print main_py
        if os.path.exists(main_py):
            print "It exists"
            build_location = self.build_location
            try:
                print build_location
                self.cpFile(main_py, build_location, localCopy)
                return True
            except Exception:
                e = traceback.format_exc()
                logging.error("Coping main.py failed: %s" % e)
                return False
        else:
            logging.error("Incorrect path for main.py %s" % main_py)
            if self.exception:
                raise "Incorrect path for main.py %s" % main_py
            return False

    def getSpecs(self, topology, localCopy=False):
        location = os.path.join(self.svn_location, topology)
        status = False
        if not os.path.exists(location):
            return status
        #### Get all .specs and  .config files
        specs_location = os.path.join(self.specs_path, topology)
        for f in os.listdir(location):
            if f.endswith(".specs") or f.endswith(".config"):
                filename = os.path.join(location, f)
                self.cpFile(filename, specs_location, localCopy)
                status = True
        return status

    def generatePretzelCommand(self, command, topology):
        if command not in ["add", "set"]:
            logging.error("invalid Pretzel command %s" % command)
            return None
        pretzel_tool = self.pretzel_tool
        specs_location = os.path.join(self.specs_path, topology)
        # If Pretzel ever become platform independent this will not be needed
        pretzel_tool = pretzel_tool.replace("/", "\\")
        specs_location = specs_location.replace("/", "\\")
        if command == "add":
            cmd = ("%s Revision -Add -Topology %s -DirectoryPath %s -Description %s" % (pretzel_tool, topology,
                                                                                        specs_location, topology))
        elif command == "set":
            cmd = ("%s Revision -SetProduction -Topology %s -Revision %s" % (pretzel_tool, topology, self.revision))
        return cmd

    def addTopology(self, topology):
        if not self.isValidTopology(topology):
            logging.error("Can't add invalid topology %s" % topology)
            if self.exception:
                raise "Can't add invalid topology %s" % topology
            return False
        else:
            cmd = self.generatePretzelCommand("add", topology)
            return self.runCommand(cmd)

    def setTopologyRevison(self, topology):
        if not self.isValidTopology(topology):
            logging.error("Can't set invalid topology %s" % topology)
            if self.exception:
                raise "Can't set invalid topology %s" % topology
            return False
        else:
            cmd = self.generatePretzelCommand("set", topology)
            self.revision += 1
            return self.runCommand(cmd)

    def testTopology(self, topology):
        status = True
        if not self.isValidTopology(topology):
            logging.error("Can't test invalid topology %s" % topology)
            if self.exception:
                raise "Can't test invalid topology %s" % topology
            return False
        else:
            status = status and self.getSpecs(topology)
            status = status and self.addTopology(topology)
            status = status and self.setTopologyRevison(topology)
            return status

class BardAdminRunner(SessionRunner):

    def __init__(self, bard_path=None, host="http://localhost:5000", logfile=None, exception=False):
        super(BardAdminRunner, self).__init__(host, logfile)
        self.exception = exception
        self.bard_path = ""
        if bard_path is not None:
            self.bard_path = bard_path

    def getModelListInfo(self, output):
        output = output.replace("<br/>", "\n")
        latest_output = output.split("\n")
        #print latest_output
        model_info = {}
        for line in latest_output:
            if len(line) > 0:
                if line.startswith("ID:"):
                    name_index = line.find("Name:")
                    id_index = line.find("ID:")
                    if name_index == -1 or id_index == -1:
                        warning_str =  "Info string incorrectly formatted '%s'" % line
                        print warning_str
                        logging.warning(warning_str)
                    else:
                        print "~"*20
                        model_id = line[id_index+4:name_index-1].strip(" ")
                        model_name = line[name_index+6:]
                        model_info[model_name] = model_id
                        print "%s : %s" % (model_name, model_id)
        return model_info

    def getLatestModelInfo(self):
        if len(self.request_text) < 1:
            warning_str =  "There is nothing in the request_text list!!!"
            print warning_str
            logging.error(warning_str)
            return None 
        return self.getModelListInfo(self.request_text[-1])

    def getModelId(self, name, model_info):
        if name in model_info:
            return model_info[name]
        else:
            warning_str =  "No such model can be found"
            print warning_str
            logging.warning(warning_str)
            return None

    def getLatestModelId(self):
        model_info = self.getLatestModelInfo()
        #print model_info
        if len(model_info.keys()) < 1:
            return None
        if len(model_info.keys()) == 1:
            return model_info[model_info.keys()[0]]
        date_dict = {}
        for name in model_info:
            try:
                t = name.split(" ")[0]
                t = t.split("_")
                #print t
                date_key = datetime.strptime("%s %s" % (t[-2], t[-1]), "%Y-%m-%d %H-%M")
                date_dict[date_key] = model_info[name]
            except ValueError:
                e = traceback.format_exc()
                print "Model name formatting is not incorrect: %s" % name
                print e
        if len(date_dict.keys()) > 0:
            return date_dict[max(date_dict.keys())]
        else:
            return None
        

    def setProperty(self, config_key, config_property, config_value):
        bard_command = os.path.join(os.path.expanduser(self.bard_path), "BardAdminTool.exe")
        bard_command = "%s ConfigStore -SetProperty -Key %s -Property %s -Value %s" % (bard_command, config_key,
                                                                                       config_property, config_value)
        bard_command = bard_command.replace("/", "\\")
        print(bard_command)
        return bard_command

    def getDocument(self, config_key):
        bard_command = os.path.join(os.path.expanduser(self.bard_path), "BardAdminTool.exe")
        bard_command = "%s ConfigStore -GetDocument -Key %s" % (bard_command, config_key)
        bard_command = bard_command.replace("/", "\\")
        print(bard_command)
        return bard_command

    def listModels(self):
        bard_command = os.path.join(os.path.expanduser(self.bard_path), "BardAdminTool.exe")
        bard_command = "%s Model -List" % bard_command
        bard_command = bard_command.replace("/", "\\")
        print(bard_command)
        return bard_command

    def downloadModels(self):
        bard_command = os.path.join(os.path.expanduser(self.bard_path), "BardAdminTool.exe")
        bard_command = "%s Model -Download" % bard_command
        bard_command = bard_command.replace("/", "\\")
        print(bard_command)
        return bard_command

    def activateModel(self, model_id):
        bard_command = os.path.join(os.path.expanduser(self.bard_path), "BardAdminTool.exe")
        bard_command = "%s Model -Activate -ID %s" % (bard_command, model_id)
        bard_command = bard_command.replace("/", "\\")
        print(bard_command)
        return bard_command

    def runSetProperty(self, config_key, config_property, config_value):
        cmd = self.setProperty(config_key, config_property, config_value)
        return self.runCommand(cmd)

    def runGetDocument(self, config_key):
        cmd = self.getDocument(config_key)
        return self.runCommand(cmd)

    def runActivateModel(self, model_id):
        cmd = self.activateModel(model_id)
        return self.runCommand(cmd)

    def runListModels(self):
        return self.runCommand(self.listModels())

    def runDownloadModels(self):
        return self.runCommand(self.downloadModels())


class UtilsRunner(SessionRunner):

    def __init__(self, host="http://localhost:5000", logfile=None, exception=False):
        super(UtilsRunner, self).__init__(host, logfile)
        self.exception = exception

    def createSchemaDir(self, original_location, file_list, tag=""):
        schema_map = {}
        if original_location.startswith("~"):
            original_location = os.path.expanduser(original_location)
        file_map = ConfigCSV[tag]
        for filename in file_list:
            if filename not in file_map:
                print "No known schema for %s" % filename
                continue
            if file_map[filename] in schema_map:
                schema_map[file_map[filename]].append(os.path.join(original_location, filename))
            else:
                schema_map[file_map[filename]] = [os.path.join(original_location, filename)]
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


class EtlRunner(PretzelRunner):

    def __init__(self, svn_location, etl_dir,
                 host="http://localhost:5000",
                 logfile=None, exception=False):
        super(EtlRunner, self).__init__(svn_location, ".", ".", host, logfile, exception)
        self.exception = exception
        self.svn_location = os.path.expanduser(svn_location)
        self.etl_dir = os.path.expanduser(etl_dir)
        self.python_code = ""
        self.etl_py_filename = "emulatePretzelETL.py"

    def generatePretzelEtlScript(self, tenant, marketting_app):
        print "Yeahh!!! Let's write some Python!!!"
        indent = " " * 4
        config_filename = "Template.config"
        specs_filename = "Template.specs"
    
        etl_settings = EtlConfig[marketting_app]
        etl_settings["DeploymentExternalID"] = tenant
        etl_settings["DataLoaderTenantName"] = tenant
    
        if marketting_app == "Marketo":
            config_filename = "Template_MKTO.config"
            specs_filename = "Template_MKTO.specs"
        if marketting_app == "Eloqua":
            config_filename = "Template_ELQ.config"
            specs_filename = "Template_ELQ.specs"
    
        # Set footer
        python_code = "%s\n\n%s\n" % ("import main", "class ETL_Settings:")
    
        # Populate ETL_Settings class
        for param in sorted(etl_settings):
            value = etl_settings[param]
            if value != "None":
                value = '"%s"' % value
            python_code += "%s%s = %s\n" % (indent, param, value)
    
        python_code += "\n%s\n%s%s\n%s%s\n" % ("class Template:",
                                               indent,'Spec = ""', 
                                               indent,'Config = ""')
    
        # Populate Template class
        python_code += "\n%s\n" % "target_template = Template()"
    
        # Populate Config
        python_code += "\n%s\n" % ('with open("%s", "r") as configfile :' % config_filename)
        python_code += "%s%s\n" % (indent, "target_template.Config = configfile.read()")
    
        # Populate Spec
        python_code += "\n%s\n" % ('with open("%s", "r") as configfile :' % specs_filename)
        python_code += "%s%s\n" % (indent, "target_template.Spec = configfile.read()")
    
        # Run ETL
        python_code += "\n%s\n" % "temp = main.ConfigureDefinition()"
        python_code += "%s\n" % "returnValue = temp.Run(ETL_Settings(), target_template)"
    
        # Save populated Spec file
        python_code += "\n%s\n" % 'specFile = open("output.specs", "w")'
        python_code += "%s\n" % "specFile.write(returnValue.Spec)"
        python_code += "%s\n" % "specFile.close()"
    
        # Save populated Config file
        python_code += "\n%s\n" % 'configFile = open("output.config", "w")'
        python_code += "%s\n" % "configFile.write(returnValue.Config)"
        python_code += "%s\n" % "configFile.close()"

        python_code += '\nprint "Ilya\'s Framework rocks!!!"\n'
        self.python_code = python_code
        return python_code

    def savePythonFile(self, python_code=None, localCopy=False):
        if python_code is None:
            python_code = self.python_code
        return self.write_to_file(self.etl_py_filename, python_code)

    def copyEtlFiles(self, topology, localCopy=False):
        etl_dir = os.path.join(self.etl_dir, topology)
        # Copy "emulatePretzelETL.py"
        if not os.path.exists(self.etl_py_filename):
            self.savePythonFile()
        self.cpFile(self.etl_py_filename, etl_dir, localCopy)

        # Copy main.py
        self.build_location = etl_dir
        self.getMain(localCopy)

        # Copy specs
        self.specs_path = self.etl_dir
        self.getSpecs(topology, localCopy)

    def verifyOutputExists(self, topology, filename):
        output = os.path.join(self.etl_dir, topology, filename)
        output = output.replace("/", "\\")
        py_eval = "os.path.exists('%s')" % output
        result = self.getEval(py_eval)
        return bool(result)

    def runEtlEmulator(self, topology, localCopy=False):
        work_dir = os.path.join(self.etl_dir, topology)
        work_dir = work_dir.replace("/", "\\")
        cmd_dict = {"python %s" % self.etl_py_filename: work_dir}
        print self.runCommand(cmd_dict, localCopy)
        print self.verifyOutputExists(topology, "output.specs")
        print self.verifyOutputExists(topology, "output.config")

    def getConfigString(self, topology):
        return self.getXmlStirng(topology, "output.config")

    def getSpecsString(self, topology):
        return self.getXmlStirng(topology, "output.specs")

    def getXmlStirng(self, topology, filename):
        output = os.path.join(self.etl_dir, topology, filename)
        output = output.replace("/", "\\")
        py_eval = "''.join(get_file_content('%s'))" % output
        print py_eval
        xml = self.getEval(py_eval)
        print type(xml)
        return xml


def main():
    #basePretzelTest()
    DLCRunner().testRun()


if __name__ == '__main__':
    main()