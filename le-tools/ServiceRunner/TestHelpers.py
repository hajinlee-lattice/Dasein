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
from TestConfigs import ConfigDLC
from TestRunner import SessionRunner

class DLCRunner(SessionRunner):

    def __init__(self, host="http://localhost:5000", logfile=None, exception=False):
        super.__init__(host, logfile)
        self.exception = exception
        self.command = ""
        self.params = {}

    def getParamsInfo(self, command):
        if command not in ConfigDLC.keys():
            logging.error("No such command [%s] in DLC" % command)
            if self.exception:
                raise "No such command [%s] in DLC" % command
            return None
        required = []
        optional = []
        for param in ConfigDLC[command].keys()[2:]:
            if ConfigDLC[command][param][0] == "required":
                required.append(ConfigDLC[command][param][0])
            elif ConfigDLC[command][param][0] == "optional":
                optional.append(ConfigDLC[command][param][0])
            else:
                logging.warning("Unknown param [%] for [%s] command" % (ConfigDLC[command][param][0], command))
        return required, optional

    def validateInput(self, command, params):
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
                logging.warning("Unknown param [%] for [%s] command" % (param, command))
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
            dlc = "dlc " + self.command
            for param in params.keys():
                dlc += " %s %s" % (param, params[param])
            return dlc
        else:
            return None
        
    def runDLCcommand(self, command, params):
        cmd = self.constructCommand(command, params)
        if cmd is None:
            logging.error("There is something wrong with your command, please see logs for details")
            if self.exception:
                raise "There is something wrong with your command, please see logs for details"
            return False
        return self.runCommand(cmd)

