import argparse
import os
from shutil import copyfile

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

LE_APPS = ['admin', 'pls', 'microservice', 'playmaker', 'oauth2', 'scoringapi']
MS_MODULES = ['dataflowapi', 'eai', 'metadata', 'modeling', 'propdata', 'scoring', 'workflowapi']

def cleanupWars():
    print 'clean up existing wars ...'
    for dirName, subdirList, fileList in os.walk(CATALINA_HOME):
        for file in fileList:
            if file[-4:] == '.war':
                print 'removing %s from %s' % (file, dirName)
                os.remove(dirName + "/" + file)
    print ''


def deployApp(app, modules):
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
        raise Error("Cannot find war file for app " + app)

    webappName = 'oauth' if (app == 'oauth2') else app
    webappWar = 'ROOT.war'
    webappDir = os.path.join(CATALINA_HOME, 'webapps', webappName, webappWar)

    copyfile(os.path.join(targetDir, appWar), webappDir)
    print 'deployed %s to %s\n' % (appWar, webappDir)


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
        raise Error("Cannot find war file for module " + module)

    webappWar = 'ROOT.war' if (module == 'core') else module + '.war'
    webappDir = os.path.join(CATALINA_HOME, 'webapps', 'ms', webappWar)
    copyfile(os.path.join(moduleDir, moduleWar), webappDir)
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
    parser.add_argument('command', type=str, help='command: deploy, cleanup, check')
    parser.add_argument('-a', dest='apps', type=str, default='microservice',
                        help='comma separated list of apps to be deployed. default is microservice. Avaiable choices are admin, pls, microservice, playmaker, oauth2, scoringapi')
    parser.add_argument('-m', dest='modules', type=str, default=','.join(MS_MODULES),
                        help='comma separated list of microservice modules to be deployed. core is implicitly included. default is all modules. Avaiable choices are dataflowapi, eai, metadata, modeling, propdata, scoring, workflowapi')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    print ''
    args = parseCliArgs()

    if args.command in ('deploy', 'cleanup'):
        cleanupWars()

    if args.command == 'deploy':
        apps = args.apps.split(',')
        modules = args.modules.split(',')

        print 'apps = %s' % apps
        print 'modules = %s\n' % modules

        for app in apps:
            deployApp(app, modules)

    if args.command in ('deploy', 'cleanup', 'check'):
        printWars()
