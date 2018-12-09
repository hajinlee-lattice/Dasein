import argparse
import atexit
import contextlib
import logging
import os
import psutil
import requests
import signal
import subprocess
import warnings
from shutil import copyfile, rmtree, copytree

logger = logging.getLogger(__name__)
ENABLE_HOT_SWAP = True

try:
    import tomcatmanager as tm
    import time
except Exception:
    ENABLE_HOT_SWAP = False

try:
    from functools import partialmethod
except ImportError:
    # Python 2 fallback: https://gist.github.com/carymrobbins/8940382
    from functools import partial


    class partialmethod(partial):
        def __get__(self, instance, owner):
            if instance is None:
                return self

            return partial(self.func, instance, *(self.args or ()), **(self.keywords or {}))

tomcatPid = None
tomcatProc = None

WSHOME = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logger.info('WSHOME=%s' % WSHOME)

CATALINA_HOME = os.environ['CATALINA_HOME']
if CATALINA_HOME is None or CATALINA_HOME == '':
    raise Error('CATALINA_HOME is not defined')
else:
    logger.info('CATALINA_HOME=%s' % CATALINA_HOME)

LE_APPS = ['admin', 'pls', 'microservice', 'playmaker', 'oauth2', 'scoringapi', 'saml', 'matchapi', 'ulysses']
MS_MODULES = ['dataflowapi', 'eai', 'metadata', 'modeling', 'propdata', 'scoring', 'workflowapi', 'quartz', 'dellebi',
              'modelquality', 'sqoop', 'datacloudapi', 'objectapi', 'cdl', 'lp']

APP_ROOT = "https://localhost"
APP_URL = {
    'microservice': "%s:9080" % APP_ROOT,
    'pls': "%s:9081" % APP_ROOT,
    'admin': "%s:9085" % APP_ROOT,
    'saml': "%s:9087" % APP_ROOT,
    'playmaker': "%s:9071" % APP_ROOT,
    'oauth2': "%s:9072" % APP_ROOT,
    'scoringapi': "%s:9073" % APP_ROOT,
    'api': "%s:9074" % APP_ROOT,
    'ulysses': "%s:9075" % APP_ROOT,
    'matchapi': "%s:9076" % APP_ROOT
}

PRESETS = {
    'cdl_pre_checkin': {
        'apps': ['admin', 'pls', 'microservice', 'matchapi', 'scoringapi'],
        'modules': ['eai', 'metadata', 'dataflowapi', 'workflowapi', 'modeling', 'scoring', 'datacloudapi', 'cdl',
                    'objectapi', 'sqoop', 'quartz', 'lp']
    },
    'lp': {
        'apps': ['admin', 'pls', 'microservice', 'oauth2', 'scoringapi', 'matchapi', 'playmaker', 'saml'],
        'modules': ['dataflowapi', 'eai', 'metadata', 'modeling', 'scoring', 'workflowapi', 'lp', 'quartz', 'sqoop',
                    'cdl']
    },
    'cdl': {
        'apps': ['admin', 'pls', 'microservice', 'playmaker', 'oauth2', 'matchapi', 'ulysses', 'saml', 'scoringapi'],
        'modules': ['dataflowapi', 'eai', 'metadata', 'workflowapi', 'modeling', 'scoring', 'datacloudapi', 'lp',
                    'quartz', 'cdl', 'objectapi', 'sqoop']
    },
    'etl': {
        'apps': ['microservice', 'matchapi'],
        'modules': ['metadata', 'workflowapi', 'datacloudapi', 'eai', 'sqoop']
    },
    'mq': {
        'apps': ['admin', 'pls', 'microservice', 'oauth2', 'scoringapi', 'matchapi', 'saml'],
        'modules': ['dataflowapi', 'eai', 'metadata', 'modeling', 'scoring', 'workflowapi', 'lp', 'quartz', 'sqoop',
                    'modelquality']
    },
    'all': {
        'apps': ['admin', 'pls', 'microservice', 'oauth2', 'scoringapi', 'matchapi', 'playmaker', 'ulysses', 'saml'],
        'modules': ['dataflowapi', 'eai', 'metadata', 'modeling', 'scoring', 'workflowapi', 'lp', 'quartz', 'sqoop',
                    'datacloudapi', 'cdl', 'objectapi']
    }
}


@contextlib.contextmanager
def no_ssl_verification():
    old_request = requests.Session.request
    requests.Session.request = partialmethod(old_request, verify=False)
    warnings.filterwarnings('ignore', 'Unverified HTTPS request')
    yield
    warnings.resetwarnings()
    requests.Session.request = old_request


def cleanupWars():
    logger.info('clean up existing wars ...')
    for dirName, subdirList, fileList in os.walk(os.path.join(CATALINA_HOME, "webapps")):
        for dir in subdirList:
            if dir == 'ROOT':
                rmtree(dirName + "/" + dir)
        for file in fileList:
            if file[-4:] == '.war':
                logger.info('removing %s from %s' % (file, dirName))
                os.remove(dirName + "/" + file)

    for module in MS_MODULES:
        dirName = os.path.join(CATALINA_HOME, "webapps", "ms")
        if (os.path.isdir(dirName + "/" + module)):
            logger.info('removing %s from %s' % (module, dirName))
            rmtree(dirName + "/" + module)

    logger.info('clean up workspace ...')
    for dir_name in os.listdir(os.path.join(CATALINA_HOME, "work")):
        dir_path = os.path.join(CATALINA_HOME, "work", dir_name)
        if os.path.isdir(dir_path):
            logger.info('cleaning up working directory %s ' % dir_path)
            rmtree(dir_path)


def deployApp(app, modules):
    logger.info('deploying ' + app)
    if app == 'microservice':
        deployMs(modules)
        return

    appWar = None
    targetDir = os.path.join(WSHOME, 'le-' + app, 'target')
    for file in os.listdir(targetDir):
        if file[-4:] == '.war':
            appWar = file
            logger.info('found %s in %s' % (file, targetDir))
            break

    if appWar is None:
        raise IOError("Cannot find war file for app " + app)

    webappName = 'oauth' if (app == 'oauth2') else app
    webappWar = 'ROOT.war'
    webappDir = os.path.join(CATALINA_HOME, 'webapps', webappName)

    deployMgrApp(app)

    webappFile = os.path.join(webappDir, webappWar)
    copyfile(os.path.join(targetDir, appWar), webappFile + ".copy")
    os.rename(webappFile + ".copy", webappFile)

    logger.info('deployed %s to %s' % (appWar, webappFile))


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
            logger.info('found %s in %s' % (moduleWar, moduleDir))
            break

    if moduleWar is None:
        raise IOError("Cannot find war file for module " + module)

    webappWar = 'ROOT.war' if (module == 'core') else module + '.war'
    webappDir = os.path.join(CATALINA_HOME, 'webapps', 'ms', webappWar)
    copyfile(os.path.join(moduleDir, moduleWar), webappDir + ".copy")
    os.rename(webappDir + ".copy", webappDir)
    logger.info('deployed %s to %s' % (moduleWar, webappDir))


def deployMgrApp(app, wait=False):
    webappName = 'oauth' if (app == 'oauth2') else app
    webappDir = os.path.join(CATALINA_HOME, 'webapps', webappName)

    if not os.path.isdir(webappDir):
        os.makedirs(webappDir)

    if not os.path.isdir(os.path.join(webappDir, 'manager')):
        if os.path.isdir(os.path.join(CATALINA_HOME, 'webapps', 'manager')):
            copytree(os.path.join(CATALINA_HOME, 'webapps', 'manager'), os.path.join(webappDir, 'manager'))
            if wait:
                for i in range(10):
                    logger.info("Wait %d sec for manager app to start ..." % (10 - i))
                    time.sleep(1)


def undeployApp(app, modules):
    global APP_URL

    deployMgrApp(app, wait=True)

    app_url = APP_URL[app]

    mgr_url = app_url + "/manager"
    with no_ssl_verification():
        tomcat = tm.TomcatManager()
        logger.info("Connecting to %s's manager app at %s ..." % (app, mgr_url))
        r = tomcat.connect(mgr_url, 'admin', 'admin')
        if r.ok:
            logger.info("Connected!")
        if app == 'microservice':
            for module in modules:
                undeployPath(tomcat, '/%s' % module)
        else:
            undeployPath(tomcat, '/')


def undeployPath(tomcat, path):
    logger.info("Undeploying %s ..." % path)
    r = tomcat.undeploy(path)
    if r.ok:
        logger.info("Successfully undeployed %s" % path)
    else:
        logger.info(r.status_message)


def printWars():
    logger.info('checking deployed wars ...')
    for dirName, subdirList, fileList in os.walk(CATALINA_HOME):
        for file in fileList:
            if file[-4:] == '.war':
                logger.info('%s/%s' % (dirName, file))


def parseApps(args):
    if args.apps or args.modules:
        apps = args.apps.split(',') if args.apps else []
        modules = args.modules.split(',') if args.modules else []
    else:
        logger.info('using preset ' + args.preset)
        apps = PRESETS[args.preset]['apps']
        modules = PRESETS[args.preset]['modules']
    if len(modules) > 0 and 'microservice' not in apps:
        apps.append('microservice')

    logger.info('apps = %s' % apps)
    logger.info('modules = %s' % modules)
    return apps, modules


def cleanup_cli(args):
    cleanupWars()
    printWars()


def check_cli(args):
    printWars()


def deploy_cli(args):
    cleanupWars()
    apps, modules = parseApps(args)
    for app in apps:
        deployApp(app, modules)
    printWars()


def swap_cli(args):
    global ENABLE_HOT_SWAP
    if not ENABLE_HOT_SWAP:
        logger.warning("Cannot import tomcat manager, hot swap functionality is disabled.")
        return

    apps, modules = parseApps(args)
    for app in apps:
        undeployApp(app, modules)

    for i in range(5):
        logger.info('Sleep %d second ...' % (5 - i))
        time.sleep(1)

    for app in apps:
        deployApp(app, modules)

    printWars()


def run_cli(args):
    runTc()
    waitTc()


def parseCliArgs():
    parser = argparse.ArgumentParser(description='Deploy wars to local tomcat')
    commands = parser.add_subparsers(help="commands")

    parser1 = commands.add_parser("cleanup")
    parser1.set_defaults(func=cleanup_cli)

    parser1 = commands.add_parser("check")
    parser1.set_defaults(func=check_cli)

    parser1 = commands.add_parser("deploy")
    parser1.add_argument('-a', dest='apps', type=str,
                         help='comma separated list of apps to be deployed. Available choices are ' + ', '.join(
                                 LE_APPS))
    parser1.add_argument('-m', dest='modules', type=str,
                         help='comma separated list of microservice modules to be deployed. core is implicitly included. Available choices are ' + ', '.join(
                                 MS_MODULES))
    parser1.add_argument('-p', dest='preset', type=str, default='lp',
                         help='preset apps/modules, choose from [' + ', '.join(
                                 PRESETS) + '], default is lp. If -a or -m is specified, this opt is ignored.')
    parser1.set_defaults(func=deploy_cli)

    parser1 = commands.add_parser("run")
    parser1.set_defaults(func=run_cli)

    parser1 = commands.add_parser("swap")
    parser1.add_argument('-a', dest='apps', type=str,
                         help='comma separated list of apps to be deployed. Available choices are ' + ', '.join(
                                 LE_APPS))
    parser1.add_argument('-m', dest='modules', type=str,
                         help='comma separated list of microservice modules to be deployed. core is implicitly included. Available choices are ' + ', '.join(
                                 MS_MODULES))
    parser1.add_argument('-p', dest='preset', type=str, default='lp',
                         help='preset apps/modules, choose from [' + ', '.join(
                                 PRESETS) + '], default is lp. If -a or -m is specified, this opt is ignored.')
    parser1.set_defaults(func=swap_cli)

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
    fmt = '%(asctime)s [%(threadName)s] %(levelname)s %(lineno)d: - %(message)s'
    dfmt = '%Y-%m-%d %H:%M:%S'
    logging.basicConfig(format=fmt, datefmt=dfmt, level=logging.INFO)

    args = parseCliArgs()
    args.func(args)
