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
from subprocess import PIPE
from subprocess import Popen

import datetime
import json
import logging
import os.path
import requests

class SessionRunner(object):
    
    def __init__(self, host="http://localhost:5000", logfile=None):
        self.host = host
        self.activity_log = {}
        self.request_text = []
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

    def getFiles(self, location, suffix_list=None):
        if not os.path.exists(location):
            return None
        file_list = []
        #### Get all files
        for f in os.listdir(location):
            if suffix_list is not None:
                suffix_check = False
                for s in suffix_list:
                    if f.endswith(s):
                        suffix_check = True
                        break
                if suffix_check:
                    file_list.append(os.path.join(location, f))
            else:
                file_list.append(os.path.join(location, f))
        return file_list

    def processFile(self, filename, process):
        request_url = self.host + "/%s" % process
        print request_url
        logging.info(request_url)
        files = {"file": open(filename, "rb")}
        request = requests.post(request_url, files=files)
        logging.info("%s\n%s" % (request.status_code, request.text))
        self.activity_log[self.stamp()] = "%s:%s" % (request.status_code, request.text)
        self.request_text.append(request.text)
        if request.status_code == 200:
            return True
        else:
            return False

    def copyFile(self, filename, location):
        status = self.processFile(filename, "upload")
        request_url = "%s/copyfile" % self.host
        print request_url
        copy_dict = {"location": location, "filename": os.path.basename(filename)}
        print copy_dict
        request = requests.post(request_url, data=json.dumps(copy_dict))
        logging.info("%s\n%s" % (request.status_code, request.text))
        self.activity_log[self.stamp()] = "%s:%s" % (request.status_code, request.text)
        self.request_text.append(request.text)
        if request.status_code == 200:
            return status and True
        else:
            return status and False

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

    def runCommand(self, cmd, local=False):
        if local:
            return self.runCommandLocally(cmd)
        else:
            return self.runCommandOnServer(cmd)

    def runCommandLocally(self, cmd, from_dir=None):
        if from_dir is None:
            from_dir = os.getcwd()
        if from_dir.startswith("~"):
            from_dir = os.path.expanduser(from_dir)
        p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True, cwd=from_dir)
        out, err = p.communicate()
        if err:
            logging.info(out)
            logging.error(err)
            print err
            return False
        else:
            logging.info(out)
            return True

    def runCommandOnServer(self, cmd):
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
            self.request_text.append(request.text)
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
            self.request_text.append(request.text)
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
            self.request_text.append(request.text)
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
            self.request_text.append(request.text)
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
 
    def testRun(self, os_type="Linux"):
        print "Starting tests. All should be True"
        cmd_list = ["pwd", "ls -lah"]
        cmd_dict = {"pwd": "~"}
        cmd_str = "ls -lah ~"
        cp_dir = "/tmp/t/m/p"
        if os_type == "Windows":
            cmd_list = ["echo %cd%", "dir"]
            cmd_dict = {"echo %cd%": "C:\\"}
            cmd_str = "dir"
            cp_dir = "C:/temp/t/m"
        self.verify(self.copyFile("tmp.log", cp_dir ), True, "0")
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
    #SessionRunner("http://localhost:5000").testRun()
    #SessionRunner("http://10.41.1.57:5000").testRun("Windows")
    SessionRunner("http://10.41.1.187:5000").testRun("Windows")

if __name__ == '__main__':
    main()