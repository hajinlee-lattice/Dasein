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
import os
import shutil
import traceback
import logging
from copy import deepcopy
from TestConfigs import ConfigDLC
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

    def isValidTopology(self, topology):
        if topology in self.topologies:
            return True
        return False

    def cpFile(self, filename, location, local=False):
        if local:
            # Local copy
            shutil.copyfile(filename, location)
        else:
            # Copy over HTTP to win server
            self.copyFile(filename, location)

    def getMain(self, localCopy=False):
        # SVN can be both on Linux and Windows, so this way:
        main_py = os.path.join(self.svn_location, "Templates", "Python script", "main.py")
        print main_py
        if os.path.exists(main_py):
            print "It exists"
            build_location = os.path.join(self.build_path, "Pretzel", "PretzelDaemon", "bin", "scripts", "production")
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
        location = os.path.join(self.svn_location,"Templates", topology)
        status = False
        if not os.path.exists(location):
            return status
        #### Get all .specs and  .config files
        for f in os.listdir(location):
            if f.endswith(".specs") or f.endswith(".config"):
                filename = os.path.join(location, f)
                self.cpFile(filename, self.specs_path, localCopy)
                status = True
        return status

    def generatePretzelCommand(self, command, topology):
        if command not in ["add", "set"]:
            logging.error("invalid Pretzel command %s" % command)
            return None
        pretzel_tool = os.path.join(self.build_path,"Pretzel", "Install", "bin", "PretzelAdminTool.exe")
        # If Pretzel ever become platform independent this will not be needed
        pretzel_tool = pretzel_tool.replace("/", "\\")
        if command == "add":
            cmd = ("%s Revision -Add -Topology %s -DirectoryPath %s -Description %s" % (pretzel_tool, topology,
                                                                                        self.specs_path, topology))
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


def main():
    #basePretzelTest()
    DLCRunner().testRun()


if __name__ == '__main__':
    main()