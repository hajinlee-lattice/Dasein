#!/usr/local/bin/python
# coding: utf-8
from __builtin__ import str
import datetime
import json
import logging
import os.path
import shutil
from subprocess import PIPE, Popen
import traceback

import pypyodbc as pyodbc
import requests
from Properties import PLSEnvironments


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


class SessionRunner(object):
    def __init__(self, logfile=None):
        self.host = PLSEnvironments.pls_test_server
        self.activity_log = {}
        self.request_text = []
        if logfile is None:
            logfile = os.path.join(os.getcwd(), "Session.log")
        if not os.path.isfile(logfile):
            logfile = os.path.join(os.getcwd(), "Session.log")
        self.logfile = logfile
        self.testStatus = True
        self.stdout = ""
        self.stderr = ""
        logging.basicConfig(filename=self.logfile, level=logging.DEBUG)

    def getQuery(self, connection_string, query):
#         try:
#             import pypyodbc
#         except Exception:
#             e = traceback.format_exc()
#             print "\nERROR:Could not import pypyodbc. it MUST be installed", e, "\n"
#             print "Linux:https://code.google.com/p/pypyodbc/wiki/Linux_ODBC_in_3_steps"
#             print "Windows:https://code.google.com/p/pyodbc/wiki/ConnectionStrings"
#             return None
        try:
            conn = pyodbc.connect(connection_string)
            cur = conn.cursor()
            cur.execute(query)
            results = cur.fetchall()
            cur.close()
            conn.close()
            return results
        except Exception:
            e = traceback.format_exc()
            print "\nFAILED:Could not fetch query results", e, "\n"
            return None
    
    def execQuery(self, connection_string, query):
        
        try:
            conn = pyodbc.connect(connection_string)
            cur = conn.cursor()
            results = cur.execute(query)            
#             results = cur.fetchall()
            cur.commit();
            cur.close()
            conn.close()
            return results
        except Exception:
            e = traceback.format_exc()
            print "\nFAILED:Could not fetch query results", e, "\n"
            return None
    
    def execProc(self, connection_string, procQuery):
        
        try:
            conn = pyodbc.connect(connection_string)
            cur = conn.cursor()
            results = cur.execute(procQuery)            
            results = cur.fetchall()
            cur.commit();
            cur.close()
            conn.close()
            return results
        except Exception:
            e = traceback.format_exc()
            print "\nFAILED:Could not fetch query results", e, "\n"
            return None
    
    def execBulkProc(self, connection_string, procQuery):
        
        try:
            conn = pyodbc.connect(connection_string)
            cur = conn.cursor()
            results = cur.execute(procQuery)            
#             results = cur.fetchall();
            cur.commit();
            cur.close()
            conn.close()
            return results
        except Exception:
            e = traceback.format_exc()
            print "\nFAILED:Could not fetch query results", e, "\n"
            return None
            
    def getSTDoutputs(self, text):
        stdo = ""
        stde = ""
        if text.find("STDOUT:") != -1:
            stdo = text[text.find("STDOUT:")+7:text.find("STDERR:")]
            stde = text[text.find("STDERR:")+7:]
        self.stdout = stdo
        self.stderr = stde
        return stdo, stde

    def getStatus(self):
        sep = "~"*25
        if self.stderr.strip("\n") != "":
            print "%s\nFAILED:%s\n%s" % (sep, self.stderr, sep)
            return False
        if self.stdout.lower().find("success") != -1:
            print "%s\nSUCCESS:%s\n%s" % (sep, self.stdout, sep)
            return True
        print "%s\nUNKNOWN: STDERR is empty and STDOUT is %s\n%s" % (sep, self.stdout, sep)
        return None

    def stamp(self):
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

    def flush(self):
        for key in sorted(self.activity_log.keys()):
            print "%s || %s" % (key, self.activity_log[key])

    def write_to_file(self, filename, updates, utf=False):
        try:
            f = open(filename, "w+")
            if type(updates) == list:
                for line in updates:
                    if utf:
                        f.write(line.encode('utf8', 'replace'))
                    else:
                        f.write(line)
            else:
                if utf:
                    f.write(updates.encode('utf8', 'replace'))
                else:
                    f.write(updates)
            f.close()
            return True
        except IOError:
            e = traceback.format_exc()
            print("Unable to modify the file: %s" % filename, e)
            return False
    
    def get_file_content(self, filename):
        content = []
        try:
            f = open(filename, "r+")
            content = f.readlines()
            f.close()
        except IOError:
            e = traceback.format_exc()
            print("Unable to read the file: %s" % filename, e)
        return content
    
    def add_file_footer(self, filename, footer):
        content = []
        content += self.get_file_content(filename)
        if type(footer) == list:
            content = content + footer
        else:
            content.append(footer) 
        self.write_to_file(filename, content)

    def getFiles(self, location, suffix_list=None):
        if location.startswith("~"):
            location = os.path.expanduser(location)
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
                    file_list.append(f)
            else:
                file_list.append(f)
        return file_list

    def getWsdlClient(self, wsdl):
        # SOAP https://qatest2.dev.lattice.local/BD2_ADEDTBDd70064254nD26163627r12/DataLoaderShim?wsdl
        """
        Example for DataLoader_Shim:
        wsdl = "https://%s/%s/DataLoaderShim?wsdl" % (soap_host, tenant)
        print wsdl
        client = Client(wsdl, headers={"MagicAuthentication": "Security through obscurity!"})
        print client
        result = client.service.GetLoadGroupStatus("ExecuteModelBuilding")
        print result
        result = client.service.ConfigureDataLoaderAndVisiDB(config_string, specs_string)
        print result
        """
        try:
            from suds.client import Client
        except Exception:
            e = traceback.format_exc()
            print "!!!ERROR: suds library must be installed for this to work: %s !!!" % e
            print "You need to run: sudo easy_install suds"
            return None
        try:
        # This is for the future use. Uncomment line below and suds import @ header
            client = Client(wsdl, headers={"MagicAuthentication": "Security through obscurity!"})
            print client
            return client
        except Exception:
            e = traceback.format_exc()
            print "FAILED: can not get SOAP  client: %s !!!" % e
            return None

    def processFile(self, filename, process):
        request_url = self.host + "/%s" % process
        print request_url
        logging.info(request_url)
        files = {"file": open(filename, "rb")}
        request = requests.post(request_url, files=files)
        logging.info("%s\n%s" % (request.status_code, request.text))
        self.activity_log[self.stamp()] = "%s:%s" % (request.status_code, request.text)
        self.request_text.append(request.text)
        self.getSTDoutputs(request.text.replace("<br/>", "\n"))
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
        self.getSTDoutputs(request.text.replace("<br/>", "\n"))
        if request.status_code == 200:
            return status and True
        else:
            return status and False

    def cpFile(self, filename, location, local=False):
        if local:
            # Local copy
            if not os.path.exists(location):
                os.makedirs(location)
            shutil.copy(filename, location)
        else:
            # Copy over HTTP to win server
            self.copyFile(filename, location)

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
        from_dir=None
        if type(cmd) == dict and cmd.values()!=None :
            from_dir=cmd.values()[0]
        if local:
            return self.runCommandLocally(cmd, from_dir)
        else:
            return self.runCommandOnServer(cmd)

    def runCommandLocally(self, cmd, from_dir=None):
        if from_dir is None:
            from_dir = os.getcwd()
        if from_dir.startswith("~"):
            from_dir = os.path.expanduser(from_dir)
        p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True, cwd=from_dir)
        self.stdout, self.stderr = p.communicate()
        if self.stderr:
            logging.info(self.stdout)
            logging.error(self.stderr)
            print self.stderr
            return False
        else:
            logging.info(self.stdout)
            print self.stdout
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
            self.getSTDoutputs(request.text.replace("<br/>", "\n"))
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
            self.getSTDoutputs(request.text.replace("<br/>", "\n"))
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
            self.getSTDoutputs(request.text.replace("<br/>", "\n"))
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
            self.getSTDoutputs(request.text.replace("<br/>", "\n"))
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