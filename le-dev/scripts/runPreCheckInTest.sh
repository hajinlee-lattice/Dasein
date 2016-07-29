#!/usr/bin/env python

import subprocess, os, stat, sys, pexpect, signal, psutil, atexit
import urllib, urllib2, time
import argparse

microservicePid = None
adminPid = None
plsPid = None
tomcatPid = None

WSHOME = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ['WSHOME'] = WSHOME

def isProcessRunning(processName):
    print "Checking if {0} is running".format(processName)
    psFormatString = "ps -ewf | grep {0} | grep -v grep"
    errorMessageFormatString = "{0} is not running! Please start {0} and retry."
    ps = subprocess.Popen(psFormatString.format(processName), shell=True, stdout=subprocess.PIPE)
    output = ps.stdout.read()
    ps.stdout.close()
    ps.wait()

    if output.find(processName) < 0:
        print errorMessageFormatString.format(processName)
        sys.exit(1)

def isEnvVariableSet(envVar):
    if envVar not in os.environ.keys():
        print "{0} is not in the list of environment variables. Please set before re-running scirpt.".format(envVar)
        sys.exit(1)

def killAllRunningServers():
    if microservicePid:
        killProcessAndAllChildProcesses(microservicePid)
    if adminPid:
        killProcessAndAllChildProcesses(adminPid)
    if plsPid:
        killProcessAndAllChildProcesses(plsPid)
    if tomcatPid:
        killProcessAndAllChildProcesses(tomcatPid)

def killProcessAndAllChildProcesses(parentPid):
    try:
      proc = psutil.Process(parentPid)
    except psutil.NoSuchProcess:
      return
    childPids = proc.children(recursive=True)
    for childPid in childPids:
      os.kill(childPid.pid, signal.SIGKILL)
    os.kill(parentPid, signal.SIGKILL)

def resetMysql():
    print "resetting mysql tables"
    subprocess.call(['bash %s/le-dev/scripts/setupdb_pls_multitenant.sh' % os.environ['WSHOME']], shell=True)
    subprocess.call(['bash %s/le-dev/scripts/setupdb_ldc_managedb.sh' % os.environ['WSHOME']], shell=True)
    subprocess.call(['bash %s/le-dev/scripts/setupdb_leadscoringdb.sh' % os.environ['WSHOME']], shell=True)
    subprocess.call(['bash %s/le-dev/scripts/setupdb_oauth2.sh' % os.environ['WSHOME']], shell=True)

def waitForServerToStart(checkingUrl, waitminute):
    startTime = time.time()

    while time.time() - startTime < (waitminute * 60):   # Wait for [waitminute] minutes for the server to start
        try:
            time.sleep(5)
            print 'checking service at %s' % checkingUrl
            output = urllib2.urlopen(checkingUrl)
            if output.getcode() == 200:
                print checkingUrl + " server started"
                return True
        except urllib2.HTTPError:
            continue
        except urllib2.URLError:
            continue

    print 'timeout on checking %s after %d seconds.' % (checkingUrl, waitminute * 60)

    return False

def startAllServers(waitminute):
    microserviceUrl = "http://localhost:8080/doc/status"
    isMicroserviceRunning = False
    try:
        restCallOutput = urllib2.urlopen(microserviceUrl).read()
        if "microservice" in restCallOutput:
            isMicroserviceRunning = True
    except urllib2.HTTPError:
        isMicroserviceRunning = False
    except urllib2.URLError:
        isMicroserviceRunning = False
    if not isMicroserviceRunning:
        print "Microservice server is not running locally. Starting on port 8080"
        os.chdir(os.environ['JETTY_HOME'])
        proc = subprocess.Popen([os.environ['WSHOME'] + '/le-dev/scripts/start-jetty.sh'])
        if proc:
            global microservicePid
            microservicePid = proc.pid
        if not waitForServerToStart(microserviceUrl, waitminute):
            print "Microservice server did not successfully start. Exitting."
            sys.exit(1)
    else:
        print "Microservice server is running locally."

    plsServerUrl = "http://localhost:8081/pls/v2/api-docs"
    isPLSServerRunning = False
    try:
        restCallOutput = urllib2.urlopen(plsServerUrl).read()
        if "Lattice Engines PLS REST API" in restCallOutput:
            isPLSServerRunning = True
    except urllib2.HTTPError:
        isPLSServerRunning = False
    except urllib2.URLError:
        isPLSServerRunning = False
    if not isPLSServerRunning:
        print "PLS server is not running locally. Starting on port 8081"
        os.chdir(os.environ['WSHOME'] + '/le-pls')
        envvars = os.environ.copy()
        envvars['MAVEN_OPTS'] = "-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=5003,server=y,suspend=n -Xmx1024m -XX:MaxPermSize=256m"
        proc = subprocess.Popen(['mvn', '-Djavax.net.ssl.trustStore=certificates/ledp_keystore.jks', '-Djetty.port=8081', '-Pfunctional', '-DLOCAL_MODEL_DL_QUARTZ_ENABLED=enabled', 'jetty:run'], env=envvars)
        if proc:
            global plsPid
            plsPid = proc.pid
        if not waitForServerToStart(plsServerUrl, waitminute):
            print "PLS server did not successfully start. Exitting."
            killAllRunningServers()
            sys.exit(1)
    else:
        print "PLS server is running locally."

    tenantConsoleUrl = "http://localhost:8085/admin/v2/api-docs"
    isTenantConsoleRunning = False
    try:
        restCallOutput = urllib2.urlopen(tenantConsoleUrl).read()
        if "Tenant Console" in restCallOutput:
            isTenantConsoleRunning = True
    except urllib2.HTTPError:
        isTenantConsoleRunning = False
    except urllib2.URLError:
        isTenantConsoleRunning = False
    if not isTenantConsoleRunning:
        print "Tenant Console server is not running locally. Starting on port 8085"
        os.chdir(os.environ['WSHOME'] + '/le-admin')
        subprocess.call(['rm', '-f', '/var/log/ledp/le-admin*.log'])
        envvars = os.environ.copy()
        envvars['MAVEN_OPTS'] = "-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=4005,server=y,suspend=n -Xmx1024m -XX:MaxPermSize=256m"
        proc = subprocess.Popen(['mvn', '-DADMIN_PROPDIR=conf/env/dev', '-Djavax.net.ssl.trustStore=certificates/ledp_keystore.jks', '-Djetty.port=8085', '-Pfunctional', '-DLOCAL_MODEL_DL_QUARTZ_ENABLED=enabled', 'jetty:run'], env=envvars)
        if proc:
            global adminPid
            adminPid = proc.pid
        if not waitForServerToStart(tenantConsoleUrl, waitminute):
            print "Tenant Console server did not successfully start. Exitting."
            killAllRunningServers()
            sys.exit(1)
    else:
        print "Tenant Console server is running locally."


def startAllServersViaTomcat(waitminute):
    print "Checking if tomcat is running"
    psString = "ps -ewf | grep org.apache.catalina.startup.Bootstrap | grep -v grep"
    ps = subprocess.Popen(psString, shell=True, stdout=subprocess.PIPE)
    output = ps.stdout.read()
    ps.stdout.close()
    ps.wait()

    if output.find('org.apache.catalina.startup.Bootstrap') >= 0:
        print "tomcat is already running."
        return

    print "starting tomcat"
    subprocess.call(['python %s/le-dev/scripts/tcdpl.py deploy -a admin,pls,microservice' % os.environ['WSHOME']], shell=True)
    proc = subprocess.Popen(['bash %s/le-dev/scripts/run-tomcat.sh' % os.environ['WSHOME']], shell=True)
    if proc:
        global tomcatPid
        tomcatPid = proc.pid

    microserviceUrl = "http://localhost:8080/doc/status"
    plsServerUrl = "http://localhost:8081/pls/v2/api-docs"
    tenantConsoleUrl = "http://localhost:8085/admin/v2/api-docs"

    if not waitForServerToStart(microserviceUrl, waitminute):
        print "Microservice server did not successfully start. Exitting."
        sys.exit(1)

    if not waitForServerToStart(plsServerUrl, waitminute):
        print "PLS server did not successfully start. Exitting."
        sys.exit(1)

    if not waitForServerToStart(tenantConsoleUrl, waitminute):
        print "Tenant Console server did not successfully start. Exitting."
        sys.exit(1)


def uploadNecessaryFilesToHDFS():
    os.chdir(os.environ['WSHOME'] + '/le-dev/testartifacts/')
    subprocess.call(["hadoop", "fs", "-rm", "-r", "/tmp/Stoplist"])
    subprocess.call(["hadoop", "fs", "-mkdir", "-p", "/tmp/Stoplist"])
    subprocess.call(['hadoop', 'fs', '-copyFromLocal', 'Stoplist/Stoplist.avro', '/tmp/Stoplist/'])

    subprocess.call(["hadoop", "fs", "-rm", "-r", "/tmp/AccountMaster/"])
    subprocess.call(["hadoop", "fs", "-mkdir", "-p", "/tmp/AccountMaster/"])
    subprocess.call(['hadoop', 'fs', '-copyFromLocal', 'AccountMaster/AccountMaster.avro', '/tmp/AccountMaster/'])

    os.chdir('models')
    subprocess.call(['tar', '-xvf', '../pdTestModels.tar'])
    os.chdir('../')
    subprocess.call(["hadoop", "fs", "-rm", "-r", "/tmp/PDEndToEndTest/"])
    subprocess.call(["hadoop", "fs", "-mkdir", "-p", "/tmp/PDEndToEndTest/"])
    subprocess.call(["hadoop", "fs", "-copyFromLocal", "models/", "/tmp/PDEndToEndTest/"])

def runPDMockedEndToEndTest():
    os.chdir(os.environ['WSHOME'] + '/le-pls')
    ret = subprocess.call(['mvn', '-Pdeployment', '-Ddeployment.groups=deployment.precheckin', '-DargLine=""', '-Djavax.net.ssl.trustStore=certificates/ledp_keystore.jks', '-Djava.util.logging.config.file=../le-dev/test-logging.properties', 'clean', 'verify', '-Dtest=PDMockedEndToEnd*'])
    assert ret == 0, 'PD EndToEnd Test Failed'

def runTestSetupScript():
    print 'running setup script le-dev/scripts/setup.sh'
    subprocess.call(['bash %s/le-dev/scripts/setup.sh' % os.environ['WSHOME']], shell=True)

def parseCliArgs():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--skip-setup', dest='skipsetup', action='store_true', help='skip test setup.')
    parser.add_argument('--skip-server', dest='skipserver', action='store_true', help='skip check and start jetty servers.')
    parser.add_argument('--tomcat', dest='tomcat', action='store_true', help='using tomcat.')
    parser.add_argument('-w', dest='waitminute', type=int, default=5, help='number of minutes wait for jettys to start up, default = 5 min.')
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    args = parseCliArgs()

    isProcessRunning("mysql")
    isProcessRunning("zookeeper")
    isProcessRunning("hadoop")

    isEnvVariableSet("WSHOME")
    if args.tomcat:
        isEnvVariableSet("CATALINA_HOME")
    else:
        isEnvVariableSet("JETTY_HOME")

    os.chdir(os.environ['WSHOME'] + '/le-db')
    resetMysql()

    uploadNecessaryFilesToHDFS()

    if not args.skipsetup:
        runTestSetupScript()
    else:
        print 'Skip test setup.'

    atexit.register(killAllRunningServers)

    if not args.skipserver:
        if args.tomcat:
            startAllServersViaTomcat(args.waitminute)
        else:
            startAllServers(args.waitminute)
    else:
        print 'Skip checking servers.'

    print 'Environmental setup finished for PD End to End. Running the actual test.'
    runPDMockedEndToEndTest();

