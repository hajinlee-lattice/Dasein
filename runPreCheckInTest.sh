#!/usr/bin/env python

import subprocess, os, stat, sys, pexpect, signal, psutil, atexit
import urllib, urllib2, time

microservicePid = None
adminPid = None
plsPid = None

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
    os.chdir(os.environ['WSHOME'] + '/le-db')
    mysqlCmd = 'mysql -u root -p < ddl_pls_multitenant_mysql5innodb.sql'
    mysqlProc = pexpect.spawn('/bin/bash', ['-c', mysqlCmd])
    ret = mysqlProc.expect('Enter password:')
    assert ret == 0, "Run command 'mysql -u root -p < ddl_pls_multitenant_mysql5innodb.sql' from le-db/ failed"
    charsWritten = mysqlProc.sendline('welcome')
    assert charsWritten == 8, "Error with running ddl_pls_multitenant_mysql5innodb.sql script in le-db directory. Please check if script is correct"


def waitForServerToStart(checkingUrl):
    startTime = time.time()

    while time.time() - startTime < 180:   # Wait for 3 minutes for the server to start
        try:
            output = urllib2.urlopen(checkingUrl)
            if output.getcode() == 200:
                print checkingUrl + " server started"
                return True
        except urllib2.HTTPError:
            continue
        except urllib2.URLError:
            continue
    return False

def startAllServers():
    microserviceUrl = "http://localhost:8080/doc/doc/api-docs"
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
        if not waitForServerToStart(microserviceUrl):
            print "Microservice server did not successfully start. Exitting."
            sys.exit(1)
    else:
        print "Microservice server is running locally."

    runjettypls='MAVEN_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=5003,server=y,suspend=n -Xmx1024m -XX:MaxPermSize=256m" mvn -Djavax.net.ssl.trustStore=certificates/laca-ldap.dev.lattice.local.jks -Djetty.port=8081 -Pfunctional -DLOCAL_MODEL_DL_QUARTZ_ENABLED=enabled jetty:run'
    plsServerUrl = "http://localhost:8081/pls/api-docs"
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
        proc = subprocess.Popen(['mvn', '-Djavax.net.ssl.trustStore=certificates/laca-ldap.dev.lattice.local.jks', '-Djetty.port=8081', '-Pfunctional', '-DLOCAL_MODEL_DL_QUARTZ_ENABLED=enabled', 'jetty:run'], env=envvars)
        if proc:
            global plsPid
            plsPid = proc.pid
        if not waitForServerToStart(plsServerUrl):
            print "PLS server did not successfully start. Exitting."
            killAllRunningServers()
            sys.exit(1)
    else:
        print "PLS server is running locally."

    tenantConsoleUrl = "http://localhost:8085/#/login"
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
        proc = subprocess.Popen(['mvn', '-DADMIN_PROPDIR=conf/env/dev', '-Djavax.net.ssl.trustStore=certificates/laca-ldap.dev.lattice.local.jks', '-Djetty.port=8085', '-Pfunctional', '-DLOCAL_MODEL_DL_QUARTZ_ENABLED=enabled', 'jetty:run'], env=envvars)
        if proc:
            global adminPid
            adminPid = proc.pid
        if not waitForServerToStart(tenantConsoleUrl):
            print "Tenant Console server did not successfully start. Exitting."
            killAllRunningServers()
            sys.exit(1)
    else:
        print "Tenant Console server is running locally."

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
    ret = subprocess.call(['mvn', '-Pdeployment', '-Ddeployment.groups=deployment.pd', '-DargLine=""', '-DPROXY_PROPDIR=../le-proxy/conf/env/dev', '-DPLS_PROPDIR=conf/env/dev', '-DSECURITY_PROPDIR=../le-security/conf/env/dev', '-DDB_PROPDIR=../le-db/conf/env/dev', '-DWORKFLOW_PROPDIR=../le-workflow/conf/env/dev', '-Djavax.net.ssl.trustStore=certificates/laca-ldap.dev.lattice.local.jks', '-Djava.util.logging.config.file=../le-dev/test-logging.properties', 'clean', 'verify', '-Dtest=PDMockedEndToEnd*'])
    assert ret == 0, 'PD EndToEnd Test Failed'

def runTestSetupScript():
    os.chdir(os.environ['WSHOME'])
    subprocess.call(['./le-dev/scripts/setup.sh'])

if __name__ == "__main__":
    isProcessRunning("mysql")
    isProcessRunning("zookeeper")
    isProcessRunning("hadoop")

    isEnvVariableSet("JETTY_HOME")
    isEnvVariableSet("WSHOME")

    os.chdir(os.environ['WSHOME'] + '/le-db')
    resetMysql()

    uploadNecessaryFilesToHDFS()
    runTestSetupScript()

    atexit.register(killAllRunningServers)
    startAllServers()

    print 'Environmental setup finished for PD End to End. Running the actual test.'
    runPDMockedEndToEndTest();

