#!/usr/local/bin/python
# coding: utf-8
from __builtin__ import str

# Base test framework

__author__ = "Illya Vinnichenko"
__copyright__ = "Copyright 2014"
__credits__ = ["Illya Vinnichenko"]
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "Illya Vinnichenko"
__email__ = "ivinnichenko@lattice-engines.com"
__status__ = "Alpha"

# import modules

import json
import os.path
import logging
import requests
import time

class SessionRunner():
    
    def __init__(self, host="http://localhost:5000", logfile=None):
        self.host = host
        self.activity_log = {}
        if logfile is None:
            logfile = os.path.join(os.getcwd(), "Session.log")
        if not os.path.isfile(logfile):
            logfile = os.path.join(os.getcwd(), "Session.log")
        self.logfile = logfile
        logging.basicConfig(filename=self.logfile, level=logging.DEBUG)

    def stamp(self):
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

    def flush(self):
        for key in sorted(self.activity_log.keys()):
            print "%s || %s" % (key, self.activity_log[key])

    def processFile(self, filename, process):
        request_url = self.host + "/%s" % process
        files = {"file": open(filename, "rb")}
        request = requests.post(request_url, files=files)
        logging.INFO(request.status_code, request.text)
        self.activity_log[self.stamp()] = "%s:%s" % (request.status_code, request.text)
        if request.status_code == 200:
            return True
        else:
            return False

    def execfileFile(self, filename):
        return self.processFile(filename, "execfile")

    def installFile(self, filename):
        return self.processFile(filename, "installfile")

    def uploadFile(self, filename):
        return self.processFile(filename, "upload")
        
    def uploadFiles(self, filenames):
        status = True
        for filename in filenames:
            status = status and self.uploadFile(filename)
        return status

    def runCommand(self, cmd):
        request_url = self.host + "/cmd"
        if type(cmd) == str or type(cmd) == list or type(cmd) == tuple or type(cmd) == unicode:
            command_dict = {"commands": cmd}
            request = requests.post(request_url, data=json.dumps(command_dict))
            logging.INFO(request.status_code, request.text)
            self.activity_log[self.stamp()] = "%s:%s" % (request.status_code, request.text)
            if request.status_code == 200:
                return True
            else:
                return False
        else:
            msg = "Bad cmd type: %s for %s" % (type(cmd), cmd)
            logging.INFO(msg)
            self.activity_log[self.stamp()] = msg
            return False

    def runEval(self, py_eval):
        request_url = self.host + "/eval"
        if type(py_eval) == str or type(py_eval) == list or type(py_eval) == unicode:
            evals_dict = {"eval": py_eval}
            request = requests.post(request_url, data=json.dumps(evals_dict))
            logging.INFO(request.status_code, request.text)
            self.activity_log[self.stamp()] = "%s:%s" % (request.status_code, request.text)
            if request.status_code == 200:
                return True
            else:
                return False
        else:
            msg = "Bad eval type: %s for %s" % (type(py_eval), py_eval)
            logging.INFO(msg)
            self.activity_log[self.stamp()] = msg
            return False

    def runExec(self, py_exec):
        request_url = self.host + "/exec"
        if type(py_exec) == str or type(py_exec) == list or type(py_exec) == unicode:
            execs_dict = {"exec": py_exec}
            request = requests.post(request_url, data=json.dumps(execs_dict))
            logging.INFO(request.status_code, request.text)
            self.activity_log[self.stamp()] = "%s:%s" % (request.status_code, request.text)
            if request.status_code == 200:
                return True
            else:
                return False
        else:
            msg = "Bad exec type: %s for %s" % (type(py_exec), py_exec)
            logging.INFO(msg)
            self.activity_log[self.stamp()] = msg
            return False


    def getEval(self, py_eval):
        request_url = self.host + "/eval"
        if not (type(py_eval) == str or type(py_eval) == unicode):
            return None
        else:
            evals_dict = {"eval": py_eval}
            request = requests.post(request_url, data=json.dumps(evals_dict))
            logging.INFO(request.status_code, request.text)
            self.activity_log[self.stamp()] = "%s:%s" % (request.status_code, request.text)
            if request.status_code == 200:
                return request.text
            else:
                return None


def main():
    test = SessionRunner("http://172.17.42.1:5000")
    
    cmd_list = ["ls -lah", "pwd"]
    cmd_tuple = ("pwd", "/home")
    cmd_str = "ls -lah ~"
    print test.runCommand(cmd_list)
    print test.runCommand(cmd_tuple)
    print test.runCommand(cmd_str)
    
    print test.uploadFile("tmp.log")
    
    print test.runEval("5+5")
    print test.runEval(["5+5", "6+6"])
    print test.getEval("7+7")
    print test.getEval(7)
    print test.runEval(("5+5", "6+6"))
    
    print test.runExec("print 2")
    
    test.flush()

if __name__ == '__main__':
    main()