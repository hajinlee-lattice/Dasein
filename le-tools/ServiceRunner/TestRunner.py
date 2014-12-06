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

import datetime
import json
import logging
import os.path
import requests

class SessionRunner(object):
    
    def __init__(self, host="http://localhost:5000", logfile=None):
        self.host = host
        self.activity_log = {}
        if logfile is None:
            logfile = os.path.join(os.getcwd(), "Session.log")
        if not os.path.isfile(logfile):
            logfile = os.path.join(os.getcwd(), "Session.log")
        self.logfile = logfile
        self.testStatus = True
        logging.basicConfig(filename=self.logfile, level=logging.DEBUG)

    def stamp(self):
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

    def flush(self):
        for key in sorted(self.activity_log.keys()):
            print "%s || %s" % (key, self.activity_log[key])

    def processFile(self, filename, process):
        request_url = self.host + "/%s" % process
        print request_url
        logging.info(request_url)
        files = {"file": open(filename, "rb")}
        request = requests.post(request_url, files=files)
        logging.info("%s\n%s" % (request.status_code, request.text))
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
        print request_url
        logging.info(request_url)
        if type(cmd) == str or type(cmd) == list or type(cmd) == dict or type(cmd) == unicode:
            command_dict = {"commands": cmd}
            if type(cmd) == dict:
                request_url = self.host + "/cmdfromdir"
            request = requests.post(request_url, data=json.dumps(command_dict))
            logging.info("%s\n%s" % (request.status_code, request.text))
            self.activity_log[self.stamp()] = "%s:%s" % (request.status_code, request.text)
            if request.status_code == 200:
                return True
            else:
                return False
        else:
            msg = "Bad cmd type: %s for %s" % (type(cmd), cmd)
            logging.info(msg)
            self.activity_log[self.stamp()] = msg
            return False

    def runEval(self, py_eval):
        request_url = self.host + "/eval"
        print request_url
        logging.info(request_url)
        if type(py_eval) == str or type(py_eval) == list or type(py_eval) == unicode:
            evals_dict = {"eval": py_eval}
            request = requests.post(request_url, data=json.dumps(evals_dict))
            logging.info("%s\n%s" % (request.status_code, request.text))
            self.activity_log[self.stamp()] = "%s:%s" % (request.status_code, request.text)
            if request.status_code == 200:
                return True
            else:
                return False
        else:
            msg = "Bad eval type: %s for %s" % (type(py_eval), py_eval)
            logging.info(msg)
            self.activity_log[self.stamp()] = msg
            return False

    def runExec(self, py_exec):
        request_url = self.host + "/exec"
        print request_url
        logging.info(request_url)
        if type(py_exec) == str or type(py_exec) == list or type(py_exec) == unicode:
            execs_dict = {"exec": py_exec}
            request = requests.post(request_url, data=json.dumps(execs_dict))
            logging.info("%s\n%s" % (request.status_code, request.text))
            self.activity_log[self.stamp()] = "%s:%s" % (request.status_code, request.text)
            if request.status_code == 200:
                return True
            else:
                return False
        else:
            msg = "Bad exec type: %s for %s" % (type(py_exec), py_exec)
            logging.info(msg)
            self.activity_log[self.stamp()] = msg
            return False


    def getEval(self, py_eval):
        request_url = self.host + "/eval"
        print request_url
        logging.info(request_url)
        if not (type(py_eval) == str or type(py_eval) == unicode):
            msg = "Bad eval type: %s for %s" % (type(py_eval), py_eval)
            logging.info(msg)
            self.activity_log[self.stamp()] = msg
            return None
        else:
            evals_dict = {"eval": py_eval}
            request = requests.post(request_url, data=json.dumps(evals_dict))
            logging.info("%s\n%s" % (request.status_code, request.text))
            self.activity_log[self.stamp()] = "%s:%s" % (request.status_code, request.text)
            if request.status_code == 200:
                return request.text
            else:
                return None

    def verify(self, exp, equals, description):
        status = True
        if exp == equals:
            logging.info("Test [%s] passed" % description)
        else:
            logging.error("Test [%s] failed" % description)
            status = False
        self.testStatus = self.testStatus and status
        return status
 
    def testRun(self):
        print "Starting tests. All should be True"
        cmd_list = ["pwd", "ls -lah"]
        cmd_dict = {"pwd": "~"}
        cmd_str = "ls -lah ~"
        self.verify(self.runCommand(cmd_dict), True, "1")
        self.verify(self.runCommand(cmd_list), True, "2")
        self.verify(self.runCommand(cmd_str), True, "3")
        self.verify(self.uploadFile("tmp.log"), True, "4")
        self.verify(self.runEval("5+5"), True, "5")
        self.verify(self.runEval(["5+5", "6+6"]), True, "6")
        self.verify(self.getEval("7+7"), "14", "7")
        self.verify(self.getEval(7), None, "8")
        self.verify(self.runEval(("5+5", "6+6")), False, "9")
        self.verify(self.runEval(["5+5", "6+6"]), True, "10")
        self.verify(self.runExec("print 2"), True, "11")
        self.verify(self.execfileFile("execFile.py"), True, "12")
        self.verify(self.getEval("FooTest()").startswith(datetime.datetime.now().strftime("%Y-%m-%d")), True, "13")
        self.verify(self.installFile("execFile.py"), True, "14")
        self.verify(self.getEval("FooTest()").startswith(datetime.datetime.now().strftime("%Y-%m-%d")), True, "15")
        #self.flush()
        print "Test status: [%s]" % self.testStatus
        return self.testStatus

def main():
    SessionRunner("http://localhost:5000").testRun()

if __name__ == '__main__':
    main()