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
from collections import OrderedDict
from flask import Flask
from flask import request
from logging import FileHandler
from subprocess import PIPE
from subprocess import Popen
import argparse
import json
import os.path
import time
import traceback

EXECUTION_DIARY = OrderedDict()
EXECUTION_DIARY[time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))] = "And so it begins..."
EXECFILES = "/tmp/execfiles"
INSTALLFILES = "/tmp/installfiles"
STYLE = """
<style type="text/css">
table {font-size:12px;color:#333333;width:100%;border-width: 1px;border-color: #ebab3a;border-collapse: collapse;}
table th {font-size:12px;background-color:#e6983b;border-width: 1px;padding: 8px;border-style: solid;border-color: #ebab3a;text-align:left;}
table tr {background-color:#f0c169;}
table td {font-size:12px;border-width: 1px;padding: 8px;border-style: solid;border-color: #ebab3a;}
</style>
"""

app = Flask(__name__)

def write_to_file(filename, updates):
    try:
        f = open(filename, "w+")
        if type(updates) == list:
            f.writelines(updates)
        else:
            f.write(updates)
        f.close()
    except IOError:
        e = traceback.format_exc()
        updateExecutionDiary("Unable to modify the file: %s" % filename, e)

def get_file_content(filename):
    content = []
    try:
        f = open(filename, "r+")
        content = f.readlines()
        f.close()
    except IOError:
        e = traceback.format_exc()
        updateExecutionDiary("Unable to modify the file: %s" % filename, e)
    return content

def add_file_footer(filename, footer):
    content = []
    content += get_file_content(filename)
    if type(footer) == list:
        content = content + footer
    else:
        content.append(footer) 
    write_to_file(filename, content)

def updateExecutionDiary(cmd, output):
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    key = "|%s| on %s" % (cmd, timestamp)
    EXECUTION_DIARY[key] = output
    app.logger.info("%s : %s" % (key, output))

def getExecutionDiary():
    return EXECUTION_DIARY

def runCmd(cmd, from_dir=None):
    if from_dir is None:
        from_dir = os.getcwd()
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True, cwd=from_dir)
    out, err = p.communicate()
    return out, err

def rmFile(removefile):
    output = "File removal attempt for %\n" % removefile
    if os.path.isfile(os.path.join(INSTALLFILES, removefile)):
        rm = os.path.join(INSTALLFILES, removefile)
        os.remove(rm)
        output += "Removed %s\n" % rm
    if os.path.isfile(os.path.join(EXECFILES, removefile)):
        rm = os.path.join(EXECFILES, removefile)
        os.remove(rm)
        output += "Removed %s\n" % rm
    if os.path.isfile(os.path.join(app.config['UPLOAD_FOLDER'], removefile)):
        rm = os.path.join(app.config['UPLOAD_FOLDER'], removefile)
        os.remove(rm)
        output += "Removed %s\n" % rm
    else:
        output += "No such file %s\n" % removefile
    return output

def getPreInstallationFiles():
    return [os.path.join(INSTALLFILES, f) for f in os.listdir(INSTALLFILES) if os.path.isfile(os.path.join(INSTALLFILES, f))]

def preinstallFiles():
    files = getPreInstallationFiles()
    for filename in files:
        execfile(filename, globals())
        output = "Executing pre-installed file [%s] \n" % filename
        updateExecutionDiary("execfile(%s)" % filename, output)

def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()

def reboot_server():
    # Debug has to be set to True for this to work
    filename = os.path.realpath(__file__)
    footer = "# Rebooting on %s\n" % time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    add_file_footer(filename, footer)
    
@app.route('/reboot', methods=['POST', 'GET'])
def reboot():
    if app.config['DEBUG'] == False:
        updateExecutionDiary("Reboot...",  "Not going to happen... Debug mode is off")
        return "Not going to happen... Debug mode is off. Try --debug next time you start the server."
    try:
        for key in EXECUTION_DIARY:
            app.logger.debug("%s : %s" % (key, EXECUTION_DIARY[key]))
        reboot_server()
        updateExecutionDiary("Reboot", "Server rebooting...")
        return "Server rebooting..."
    except Exception:
        e = traceback.format_exc()
        updateExecutionDiary("Reboot...", e)
        return e

@app.route('/shutdown', methods=['POST', 'GET'])
def shutdown():
    try:
        for key in EXECUTION_DIARY:
            app.logger.debug("%s : %s" % (key, EXECUTION_DIARY[key]))
        shutdown_server()
        updateExecutionDiary("Shutdown"," Server shutting down...")
        return "Server shutting down..."
    except Exception:
        e = traceback.format_exc()
        updateExecutionDiary("Shutdown...", e)
        return e

@app.route('/')
def index():
    html = "<html>%s" % STYLE
    html += "<h2>This is REST Command Line Executor v1.0</h2><hr>"
    html += "<table border='1'><tr><th><h2>Installed Python Libraries</h2></th></tr>"
    out = runCmd("pip list")
    html+= "<tr><td><b>%s</b></td></tr>" % out[0]
    html += "</table><hr>"
    
    html += "<table border='1'><tr><th><h2>Pre-Installation Python files</h2></th></tr>"
    for f in getPreInstallationFiles():
        html+= "<tr><td><b>%s</b></td></tr>" % f
    html += "</table><hr>"
    
    html += "<table border='1'><tr><th><h2>Env Variable</h2></th><th><h2>Value</h2></th></tr>"
    for key in sorted(os.environ.keys()):
        html += "<tr><td><b>%s</b></td><td>%s</td></tr>" % (key, os.getenv(key))
    html += "</table></html>"
    return html

@app.route('/execution_diary')
def showDiary():
    html = "<html>%s" % STYLE
    html += "<table border='1'><tr><th><h2>Command</h2></th><th><h2>Output</h2></th></tr>"
    for key in EXECUTION_DIARY:
        html += "<tr><td><b>%s</b></td><td>%s</td></tr>" % (key, EXECUTION_DIARY[key].replace("\n", "<br>"))
    html += "</table></html>"
    return html

#TODO: CMD FROM DIR /cmd_from_dir

@app.route('/cmdfromdir', methods = ['POST', 'GET'])
def runJsonCommandFromDir():
    try:
        print(request.data)
        jdata = json.loads(request.data)
        if "commands" not in jdata:
            raise "No commands parameter in JSON request body."
        cmd = jdata["commands"]
        print cmd
        print type(cmd)
        if ((type(cmd) == str) or (type(cmd) == unicode)):
            print "You should have used /cmd instead of /cmdfromdir, since no 'from dir'"
            return runCommand(cmd)
        elif type(cmd) == tuple:
            return runCommand(cmd[0], cmd[1])
        elif type(cmd) == list:
            output = ""
            for cl in cmd:
                if ((type(cl) == str) or (type(cl) == unicode)):
                    print "You should have used /cmd instead of /cmdfromdir, since no 'from dir'"
                    out, err = runCmd(cl)
                    print cl, out, err
                    output += "%s\nSTDOUT:%s\nSTDERR:%s\n" % (cl, out, err)
                    updateExecutionDiary(cl, "STDOUT:%s\nSTDERR:%s\n" % (out, err))
                elif type(cl) == tuple:
                    out, err = runCommand(cl[0], cl[1])
                    print cl, out, err
                    output += "%s\nSTDOUT:%s\nSTDERR:%s\n" % (cl, out, err)
                    updateExecutionDiary(cl, "STDOUT:%s\nSTDERR:%s\n" % (out, err))
                else:
                    output += "Unsupported request%s\n" % cl
                    updateExecutionDiary(cl, output) 
            return output
        else:
            return "Unsupported request %s" % request.data
    except Exception:
        e = traceback.format_exc()
        updateExecutionDiary(str(request.data), e)
        return e


#TODO: eval

@app.route('/eval', methods = ['POST', 'GET'])
def runJsonEval():
    try:
        print(request.data)
        jdata = json.loads(request.data)
        if "eval" not in jdata:
            raise "No eval parameter in JSON request body."
        pycode = jdata["eval"]
        print pycode
        if ((type(pycode) == str) or (type(pycode) == unicode)):
            return eval(pycode)
        if type(pycode) == list:
            output = ""
            for line in pycode:
                if not ((type(line) == str) or (type(line) == unicode)):
                    continue
                stdout = eval(line)
                output += "%s\n" % stdout
                updateExecutionDiary(line, "%s\n" % stdout)
            return output
    except Exception:
        e = traceback.format_exc()
        updateExecutionDiary(str(request.data), e)
        return e

#TODO: exec
@app.route('/exec', methods = ['POST', 'GET'])
def runJsonExec():
    try:
        print(request.data)
        jdata = json.loads(request.data)
        if "exec" not in jdata:
            raise "No exec parameter in JSON request body."
        pycode = jdata["exec"]
        print pycode
        if ((type(pycode) == str) or (type(pycode) == unicode)):
            exec(pycode)
            return "Executing %s" % pycode
        if type(pycode) == list:
            output = ""
            for line in pycode:
                if not ((type(line) == str) or (type(line) == unicode)):
                    continue
                output += "%s\n" % line
                updateExecutionDiary(line, "Executing\n")
            return output
    except Exception:
        e = traceback.format_exc()
        updateExecutionDiary(str(request.data), e)
        return e
"""
>>> exec("import os.path")
>>> os.path.join("~","test")
'~/test'
>>> def getFoo(): return 5
... 
>>> eval("getFoo()")
5
>>> compile("getFoo()", "", "eval")
<code object <module> at 0x7f2c510c18b0, file "", line 1>
>>> x = compile("getFoo()", "", "eval")
>>> x
<code object <module> at 0x7f2c510c11b0, file "", line 1>
>>> x()
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
TypeError: 'code' object is not callable
>>> exec(x)
>>> print exec(x)
  File "<stdin>", line 1
    print exec(x)
             ^
SyntaxError: invalid syntax
>>> x = eval("getFoo()")
>>> print x
5

"""

@app.route('/cmd', methods = ['POST', 'GET'])
def runJsonCommand():
    try:
        print(request.data)
        jdata = json.loads(request.data)
        if "commands" not in jdata:
            raise "No commands parameter in JSON request body."
        cmd = jdata["commands"]
        print cmd
        print type(cmd)
        if ((type(cmd) == str) or (type(cmd) == unicode)):
            return runCommand(cmd)
        if type(cmd) == list:
            output = ""
            for cl in cmd:
                if not ((type(cl) == str) or (type(cl) == unicode)):
                    continue
                out, err = runCmd(cl)
                print cl, out, err
                output += "%s\nSTDOUT:%s\nSTDERR:%s\n" % (cl, out, err)
                updateExecutionDiary(cl, "STDOUT:%s\nSTDERR:%s\n" % (out, err))
            return output
    except Exception:
        e = traceback.format_exc()
        updateExecutionDiary(str(request.data), e)
        return e
    
@app.route('/cmd/<cmd>', methods = ['POST', 'GET'])
def runCommand(cmd):
    try:
        if ((type(cmd) == str) or (type(cmd) == unicode)):
            out, err = runCmd(cmd)
            output = "%s\nSTDOUT:%s\nSTDERR:%s\n" % (cmd, out, err)
            html = output.replace("\n","<br/>")
            updateExecutionDiary(cmd, output)
            return html
        else:
            raise "CMD should be a string"     
    except Exception:
        e = traceback.format_exc()
        updateExecutionDiary(cmd, e)
        return e
    
@app.route('/upload', methods = ['POST'])
def upload():
    try:
        if "file" in request.files:
            f = request.files["file"]
            uploadedFilePath = os.path.join(app.config['UPLOAD_FOLDER'], f.filename) 
            f.save(uploadedFilePath)
            output = "File uploaded to " + uploadedFilePath
            updateExecutionDiary("File upload [%s]" % f.filename, output)
        else:
            output = "No 'file' in request or you've tried to access this page manually (which you should not do)"
            updateExecutionDiary("File upload attempt", output)
        return output
    except Exception:
        e = traceback.format_exc()
        updateExecutionDiary("File Upload", e)
        return e

@app.route('/execfile', methods = ['POST'])
def execFile():
    try:
        if "file" in request.files:
            f = request.files["file"]
            uploadedFilePath = os.path.join(EXECFILES, f.filename) 
            f.save(uploadedFilePath)
            output = "File uploaded to %s \n" % uploadedFilePath
            updateExecutionDiary("File upload [%s]" % f.filename, output)
            execfile(uploadedFilePath, globals())
            output = "%s executed for this instance only \n" % uploadedFilePath
            updateExecutionDiary("execfile(%s)" % f.filename, output)
        else:
            output = "No 'file' in request or you've tried to access this page manually (which you should not do)"
            updateExecutionDiary("File upload/execfile attempt", output)
        return output
    except Exception:
        e = traceback.format_exc()
        updateExecutionDiary("ExecFile", e)
        return e

@app.route('/installfile', methods = ['POST'])
def installFile():
    try:
        if "file" in request.files:
            f = request.files["file"]
            uploadedFilePath = os.path.join(INSTALLFILES, f.filename) 
            f.save(uploadedFilePath)
            output = "File uploaded to %s \n" % uploadedFilePath
            updateExecutionDiary("File upload [%s]" % f.filename, output)
            execfile(uploadedFilePath, globals())
            output = "%s executed for this instance and saved for future use \n" % uploadedFilePath
            updateExecutionDiary("execfile(%s)" % f.filename, output)
        else:
            output = "No 'file' in request or you've tried to access this page manually (which you should not do)"
            updateExecutionDiary("File upload/execfile attempt", output)
        return output
    except Exception:
        e = traceback.format_exc()
        updateExecutionDiary("InstallFile", e)
        return e

@app.route('/removefile/<removefile>', methods = ['POST'])
def removeFile(removefile):
    try:
        output = rmFile(removefile)
        if os.path.isfile(os.path.join(INSTALLFILES, removefile)):
            rm = os.path.join(INSTALLFILES, removefile)
            os.remove(rm)
            output = "Removed %s" % rm
            updateExecutionDiary("rm %s" % rm, output)
        if os.path.isfile(os.path.join(EXECFILES, removefile)):
            rm = os.path.join(EXECFILES, removefile)
            os.remove(rm)
            output = "Removed %s" % rm
            updateExecutionDiary("rm %s" % rm, output)
        if os.path.isfile(os.path.join(app.config['UPLOAD_FOLDER'], removefile)):
            rm = os.path.join(app.config['UPLOAD_FOLDER'], removefile)
            os.remove(rm)
            output = "Removed %s" % rm
            updateExecutionDiary("rm %s" % rm, output)
        else:
            output = "No such file %s" % removefile
            updateExecutionDiary("File removal attempt", output)
        return output
    except Exception:
        e = traceback.format_exc()
        updateExecutionDiary("Remove File %s" % removefile, e)
        return e

@app.route('/removefile', methods = ['POST'])
def removeFileJson():
    try:
        print(request.data)
        jdata = json.loads(request.data)
        if "remove" not in jdata:
            raise "No commands parameter in JSON request body."
        removefile = jdata["remove"]
        if type(removefile) == str:
            return removeFile(removefile)
        if type(removefile) == list:
            try:
                for rm in removefile:
                    output = rmFile(rm)
                    updateExecutionDiary("File removal attempt", output)
            except Exception:
                e = traceback.format_exc()
                updateExecutionDiary("Remove File %s" % removefile, e)
                return e
        return updateExecutionDiary(str(request.data), "JSON based removal")
    except Exception:
        e = traceback.format_exc()
        updateExecutionDiary(str(request.data), e)
        return e


def main():
    """
    Here we have processing for installation_type and log_file args
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", nargs=1, default="0.0.0.0", help="Hostname for the server")
    parser.add_argument("--port", nargs=1, default="5000", help="Hostname for the server")
    parser.add_argument("--upload_folder", nargs=1, default="/tmp", help="Upload folder on the server")
    parser.add_argument("--config_file", nargs=1, default="config.py", help="Config file (on the server side)")
    parser.add_argument('--debug', dest='debug', action='store_true')
    parser.add_argument('--no-debug', dest='debug', action='store_false')
    parser.set_defaults(debug=True)
    #parser.add_argument("logfile", help="Log file for output")
    args = parser.parse_args()
    
    # Load the config
    #execfile(args.config_file, globals())
    global INSTALLFILES
    global EXECFILES
    EXECFILES = os.path.join(args.upload_folder, "EXECFILES")
    INSTALLFILES = os.path.join(args.upload_folder, "INSTALLFILES")
    if not os.path.isdir(args.upload_folder):
        os.mkdir(args.upload_folder)
    if not os.path.isdir(EXECFILES):
        os.mkdir(EXECFILES)
    if not os.path.isdir(INSTALLFILES):
        os.mkdir(INSTALLFILES)
    preinstallFiles()
    
    file_handler = FileHandler(os.path.join(args.upload_folder,"SessionRunner.log"))
    app.logger.addHandler(file_handler)
    
    app.config['UPLOAD_FOLDER'] = args.upload_folder
    app.run(host=args.host, port=int(args.port), debug=args.debug)
    file_handler.close()


if __name__ == '__main__':
    main()

