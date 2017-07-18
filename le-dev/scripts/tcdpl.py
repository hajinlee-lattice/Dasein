import argparse
import atexit
import os
import psutil
import signal
import subprocess
import time
from shutil import copyfile, rmtree, copytree

tomcatPid = None
tomcatProc = None

WSHOME = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print 'WSHOME=%s' % WSHOME

CATALINA_HOME = os.environ['CATALINA_HOME']
if CATALINA_HOME is None or CATALINA_HOME == '':
    raise Error('CATALINA_HOME is not defined')
else:
    print 'CATALINA_HOME=%s' % CATALINA_HOME

HADOOP_COMMON_JAR = os.environ['HADOOP_COMMON_JAR']
if HADOOP_COMMON_JAR is None or HADOOP_COMMON_JAR == '':
    raise Error('HADOOP_COMMON_JAR is not defined')
else:
    print 'HADOOP_COMMON_JAR=%s' % HADOOP_COMMON_JAR

LE_APPS = ['admin', 'pls', 'microservice', 'playmaker', 'oauth2', 'scoringapi', 'saml', 'matchapi', 'ulysses']
MS_MODULES = ['dataflowapi', 'eai', 'metadata', 'modeling', 'propdata', 'scoring', 'workflowapi', 'quartz', 'dellebi',
              'modelquality', 'sqoop', 'datacloudapi', 'objectapi', 'dante', 'cdl']

PRESETS = {
    'lp': {
        'apps': ['admin', 'pls', 'microservice', 'playmaker', 'oauth2', 'scoringapi', 'matchapi'],
        'modules': ['dataflowapi', 'eai', 'metadata', 'modeling', 'scoring', 'workflowapi', 'quartz', 'sqoop', 'dante']
    },
    'cdl': {
        'apps': ['admin', 'pls', 'microservice', 'matchapi'],
        'modules': ['dataflowapi', 'eai', 'metadata', 'workflowapi', 'modeling', 'datacloudapi', 'cdl']
    },
    'etl': {
        'apps': ['microservice', 'matchapi'],
        'modules': ['metadata', 'workflowapi', 'datacloudapi', 'eai']
    },
    'mq': {
        'apps': ['admin', 'pls', 'microservice', 'playmaker', 'oauth2', 'scoringapi', 'matchapi'],
        'modules': ['dataflowapi', 'eai', 'metadata', 'modeling', 'scoring', 'workflowapi', 'quartz', 'sqoop',
                    'modelquality']
    }
}


def cleanupWars():
    print 'clean up existing wars ...'
    for dirName, subdirList, fileList in os.walk(os.path.join(CATALINA_HOME, "webapps")):
        for dir in subdirList:
            if dir == 'ROOT':
                rmtree(dirName + "/" + dir)
        for file in fileList:
            if file[-4:] == '.war':
                print 'removing %s from %s' % (file, dirName)
                os.remove(dirName + "/" + file)

    for module in MS_MODULES:
        dirName = os.path.join(CATALINA_HOME, "webapps", "ms")
        if (os.path.isdir(dirName + "/" + module)):
            print 'removing %s from %s' % (module, dirName)
            rmtree(dirName + "/" + module)

    print 'clean up workspace ...'
    for dir_name in os.listdir(os.path.join(CATALINA_HOME, "work")):
        dir_path = os.path.join(CATALINA_HOME, "work", dir_name)
        if os.path.isdir(dir_path):
            print 'cleaning up working directory %s ' % dir_path
            rmtree(dir_path)
    print ''


def deployApp(app, modules):
    print 'deploying ' + app
    if app == 'microservice':
        deployMs(modules)
        return

    appWar = None
    targetDir = os.path.join(WSHOME, 'le-' + app, 'target')
    for file in os.listdir(targetDir):
        if file[-4:] == '.war':
            appWar = file
            print 'found %s in %s' % (file, targetDir)
            break

    if appWar is None:
        raise IOError("Cannot find war file for app " + app)

    webappName = 'oauth' if (app == 'oauth2') else app
    webappWar = 'ROOT.war'
    webappDir = os.path.join(CATALINA_HOME, 'webapps', webappName)

    if not os.path.isdir(webappDir):
        os.makedirs(webappDir)

    if not os.path.isdir(os.path.join(webappDir, 'manager')):
        if os.path.isdir(os.path.join(CATALINA_HOME, 'webapps', 'manager')):
            copytree(os.path.join(CATALINA_HOME, 'webapps', 'manager'), os.path.join(webappDir, 'manager'))

    webappFile = os.path.join(webappDir, webappWar)
    copyfile(os.path.join(targetDir, appWar), webappFile + ".copy")
    os.rename(webappFile + ".copy", webappFile)

    print 'deployed %s to %s\n' % (appWar, webappFile)


def deployMs(mods):
    for module in ['core'] + mods:
        deployMsModule(module)


def deployMsModule(module):
    MSHOME = os.path.join(WSHOME, 'le-microservice')

    moduleDir = os.path.join(MSHOME, module, 'target')
    moduleWar = None
    for fn in os.listdir(moduleDir):
        if fn[-4:] == '.war':
            moduleWar = fn
            print 'found %s in %s' % (moduleWar, moduleDir)
            break

    if moduleWar is None:
        raise IOError("Cannot find war file for module " + module)

    webappWar = 'ROOT.war' if (module == 'core') else module + '.war'
    webappDir = os.path.join(CATALINA_HOME, 'webapps', 'ms', webappWar)
    copyfile(os.path.join(moduleDir, moduleWar), webappDir + ".copy")
    os.rename(webappDir + ".copy", webappDir)
    print 'deployed %s to %s\n' % (moduleWar, webappDir)


def printWars():
    print 'checking deployed wars ...'
    for dirName, subdirList, fileList in os.walk(CATALINA_HOME):
        for file in fileList:
            if file[-4:] == '.war':
                print '%s/%s' % (dirName, file)
    print ''


def parseCliArgs():
    parser = argparse.ArgumentParser(description='Deploy wars to local tomcat')
    parser.add_argument('command', type=str, help='command: deploy, cleanup, check, run')
    parser.add_argument('-a', dest='apps', type=str,
                        help='comma separated list of apps to be deployed. Available choices are ' + ', '.join(LE_APPS))
    parser.add_argument('-m', dest='modules', type=str,
                        help='comma separated list of microservice modules to be deployed. core is implicitly included. Available choices are ' + ', '.join(
                                MS_MODULES))
    parser.add_argument('-p', dest='preset', type=str, default='lp',
                        help='preset apps/modules, choose from [' + ', '.join(
                                PRESETS) + '], default is lp. If -a or -m is specified, this opt is ignored.')
    args = parser.parse_args()

    return args


def runTc():
    proc = subprocess.Popen(['bash %s/le-dev/scripts/run-tomcat.sh' % os.environ['WSHOME']], shell=True)
    if proc:
        global tomcatPid
        tomcatPid = proc.pid
        atexit.register(killTc)


def waitTc():
    global tomcatPid
    proc = psutil.Process(tomcatPid)
    proc.wait()


def killTc():
    global tomcatPid
    try:
        proc = psutil.Process(tomcatPid)
    except psutil.NoSuchProcess:
        return
    childPids = proc.children(recursive=True)
    for childPid in childPids:
        os.kill(childPid.pid, signal.SIGKILL)
    os.kill(tomcatPid, signal.SIGKILL)


if __name__ == '__main__':
    print ''
    args = parseCliArgs()

    if args.command in ('deploy', 'cleanup', 'run'):
        cleanupWars()

    if args.command == 'run':
        runTc()
        for i in xrange(10):
            print 'wait %d sec for server to start' % (10 - i)
            time.sleep(1)

    if args.command in ('deploy', 'run'):
        if args.apps or args.modules:
            apps = args.apps.split(',') if args.apps else []
            modules = args.modules.split(',') if args.modules else []
        else:
            print 'using preset ' + args.preset
            apps = PRESETS[args.preset]['apps']
            modules = PRESETS[args.preset]['modules']
        if len(modules) > 0 and 'microservice' not in apps:
            apps.append('microservice')

        print 'apps = %s' % apps
        print 'modules = %s\n' % modules

        for app in apps:
            deployApp(app, modules)

    if args.command in ('deploy', 'cleanup', 'check', 'run'):
        printWars()

    if args.command == 'run':
        waitTc()
