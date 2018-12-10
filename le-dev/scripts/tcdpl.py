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
MS_MODULES = ['ms-core', 'dataflowapi', 'eai', 'metadata', 'modeling', 'propdata', 'scoring', 'workflowapi', 'quartz', 'dellebi',
              'modelquality', 'sqoop', 'datacloudapi', 'objectapi', 'cdl', 'lp']

LE_APPS = [ app for app in LE_APPS + MS_MODULES if app != 'microservice' ]

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
    'cdl': {
        'apps': ['admin', 'pls', 'lp', 'cdl', 'matchapi', 'metadata',
                 'scoringapi', 'dataflowapi', 'workflowapi', 'objectapi',
                 'eai', 'modeling', 'scoring', 'datacloudapi',
                 'sqoop', 'quartz']
    },
    'lp': {
        'apps': ['admin', 'pls', 'lp', 'cdl', 'oauth2', 'saml', 'scoringapi', 'matchapi', 'metadata',
                 'playmaker', 'dataflowapi', 'eai', 'modeling', 'scoring', 'workflowapi',
                 'quartz', 'sqoop']
    },
    'etl': {
        'apps': ['microservice', 'matchapi', 'metadata', 'workflowapi', 'datacloudapi', 'eai', 'sqoop']
    },
    'mq': {
        'apps': ['admin', 'pls', 'oauth2', 'scoringapi', 'matchapi', 'saml',
                 'dataflowapi', 'eai', 'metadata', 'modeling', 'scoring', 'workflowapi',
                 'lp', 'quartz', 'sqoop', 'modelquality']
    },
    'all': {
        'apps': ['admin', 'pls', 'oauth2', 'scoringapi', 'matchapi', 'playmaker', 'ulysses', 'saml',
                 'dataflowapi', 'eai', 'metadata', 'modeling', 'scoring', 'workflowapi', 'lp', 'quartz',
                 'sqoop', 'datacloudapi', 'cdl', 'objectapi']
    }
}

PRESETS['cdl_pre_checkin'] = PRESETS['cdl']


@contextlib.contextmanager
def no_ssl_verification():
    old_request = requests.Session.request
    requests.Session.request = partialmethod(old_request, verify=False)
    warnings.filterwarnings('ignore', 'Unverified HTTPS request')
    yield
    warnings.resetwarnings()
    requests.Session.request = old_request


def get_webapp_dir_in_tc(app):
    webapp = 'oauth' if (app == 'oauth2') else app
    if webapp in MS_MODULES:
        if webapp == "ms-core":
            webapp_dir = os.path.join(CATALINA_HOME, "webapps", "ms", "ROOT")
        else:
            webapp_dir = os.path.join(CATALINA_HOME, "webapps", "ms", webapp)
    else:
        webapp_dir = os.path.join(CATALINA_HOME, 'webapps', webapp)
    return webapp_dir


def cleanup_webapp_in_tc(app):
    webapp_dir = get_webapp_dir_in_tc(app)
    if os.path.isdir(webapp_dir):
        logger.info('removing %s' % webapp_dir)
        rmtree(webapp_dir)
    war_file = webapp_dir + ".war"
    if os.path.isfile(war_file):
        logger.info('removing %s' % war_file)
        os.remove(war_file)


def cleanup_wars():
    logger.info('clean up existing wars ...')
    for app in LE_APPS:
        cleanup_webapp_in_tc(app)

    # special treatment for microservice core
    cleanup_webapp_in_tc('ms-core')

    logger.info('clean up workspace ...')
    for dir_name in os.listdir(os.path.join(CATALINA_HOME, "work")):
        dir_path = os.path.join(CATALINA_HOME, "work", dir_name)
        if os.path.isdir(dir_path):
            logger.info('cleaning up working directory %s ' % dir_path)
            rmtree(dir_path)


def deploy_app(app):
    logger.info('deploying ' + app)
    # deploy manager app if not already
    deploy_mgr_app(app)
    # deploy war from maven project to tomcat
    prj_war = find_war_in_project(app)
    if prj_war is not None:
        cleanup_webapp_in_tc(app)
        webapp_dir = get_webapp_dir_in_tc(app)
        webapp_war = webapp_dir + ".war"
        copyfile(prj_war, webapp_war + ".copy")
        os.rename(webapp_war + ".copy", webapp_war)
        logger.info('deployed %s to %s' % (prj_war, webapp_war))
    else:
        logger.warning("Cannot find war file built for %s" % app)


def find_war_in_project(app):
    prj = get_project(app)
    mvn_tgt = os.path.join(prj, 'target')
    app_war = None
    for file in os.listdir(mvn_tgt):
        if file[-4:] == '.war':
            app_war = file
            logger.info('found %s in %s' % (app_war, mvn_tgt))
            break
    return os.path.join(mvn_tgt, app_war)


def get_project(app):
    if app in MS_MODULES:
        return os.path.join('le-microservice', app)
    else:
        return 'le-%s' % app


def get_mgr_app(app):
    if app == "oauth2":
        mgr_app = 'oauth'
    elif app in MS_MODULES:
        mgr_app = 'ms'
    else:
        mgr_app = app
    return mgr_app


def deploy_mgr_app(app, wait=False):
    mgr_app = get_mgr_app(app)
    mgr_app_dir = os.path.join(CATALINA_HOME, 'webapps', mgr_app)
    if not os.path.isdir(mgr_app_dir):
        os.makedirs(mgr_app_dir)
    if not os.path.isdir(os.path.join(mgr_app_dir, 'manager')):
        if os.path.isdir(os.path.join(CATALINA_HOME, 'webapps', 'manager')):
            copytree(os.path.join(CATALINA_HOME, 'webapps', 'manager'), os.path.join(mgr_app_dir, 'manager'))
            if wait:
                for i in range(10):
                    logger.info("Wait %d sec for manager app to start ..." % (10 - i))
                    time.sleep(1)


def undeploy_app(app):
    global APP_URL

    deploy_mgr_app(app, wait=True)

    app_url = APP_URL[app]

    mgr_url = app_url + "/manager"
    with no_ssl_verification():
        tomcat = tm.TomcatManager()
        logger.info("Connecting to %s's manager app at %s ..." % (app, mgr_url))
        r = tomcat.connect(mgr_url, 'admin', 'admin')
        if r.ok:
            logger.info("Connected!")
        if app in MS_MODULES and app != 'ms-core':
            undeploy_path(tomcat, '/%s' % app)
        else:
            undeploy_path(tomcat, '/')


def undeploy_path(tomcat, path):
    logger.info("Undeploying %s ..." % path)
    r = tomcat.undeploy(path)
    if r.ok:
        logger.info("Successfully undeployed %s" % path)
    else:
        logger.info(r.status_message)


def print_wars():
    logger.info('checking deployed wars ...')
    for dirName, subdirList, fileList in os.walk(CATALINA_HOME):
        for file in fileList:
            if file[-4:] == '.war':
                logger.info('%s/%s' % (dirName, file))


def run_tc():
    proc = subprocess.Popen(['bash %s/le-dev/scripts/run-tomcat.sh' % os.environ['WSHOME']], shell=True)
    if proc:
        global tomcatPid
        tomcatPid = proc.pid
        atexit.register(kill_tc)


def wait_tc():
    global tomcatPid
    proc = psutil.Process(tomcatPid)
    proc.wait()


def kill_tc():
    global tomcatPid
    try:
        proc = psutil.Process(tomcatPid)
    except psutil.NoSuchProcess:
        return
    childPids = proc.children(recursive=True)
    for childPid in childPids:
        os.kill(childPid.pid, signal.SIGKILL)
    os.kill(tomcatPid, signal.SIGKILL)


def parse_apps(args):
    if args.apps or args.modules:
        apps = args.apps.split(',') if args.apps else []
        modules = args.modules.split(',') if args.modules else []
        apps = [ app for app in apps + modules if app != 'microservice' ]
    else:
        logger.info('using preset ' + args.preset)
        if args.preset == 'cdl_pre_checkin':
            logger.warning("WARNING: preset \"cdl_pre_checkin\" is deprecated, please use \"cdl\" instead.")
        apps = PRESETS[args.preset]['apps']
    logger.info('apps = %s' % apps)
    return apps


def cleanup_cli(args):
    cleanup_wars()
    print_wars()


def check_cli(args):
    print_wars()


def deploy_cli(args):
    cleanup_wars()
    apps = parse_apps(args)
    for app in apps:
        deploy_app(app)
    print_wars()


def swap_cli(args):
    global ENABLE_HOT_SWAP
    if not ENABLE_HOT_SWAP:
        logger.warning("Cannot import tomcat manager, hot swap functionality is disabled.")
        return

    apps = parse_apps(args)
    for app in apps:
        undeploy_app(app)

    for i in range(5):
        logger.info('Sleep %d second ...' % (5 - i))
        time.sleep(1)

    for app in apps:
        deploy_app(app)

    print_wars()


def run_cli(args):
    run_tc()
    wait_tc()


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
    parser1.add_argument('-m', dest='modules', type=str, help='DEPRECATED')
    parser1.add_argument('-p', dest='preset', type=str, default='lp',
                         help='preset apps/modules, choose from [' + ', '.join(
                                 PRESETS) + '], default is lp. If -a is specified, this opt is ignored.')
    parser1.set_defaults(func=deploy_cli)

    parser1 = commands.add_parser("run")
    parser1.set_defaults(func=run_cli)

    parser1 = commands.add_parser("swap")
    parser1.add_argument('-a', dest='apps', type=str,
                         help='comma separated list of apps to be deployed. Available choices are ' + ', '.join(
                                 LE_APPS))
    parser1.add_argument('-m', dest='modules', type=str, help='DEPRECATED')
    parser1.add_argument('-p', dest='preset', type=str, default='lp',
                         help='preset apps/modules, choose from [' + ', '.join(
                                 PRESETS) + '], default is lp. If -a is specified, this opt is ignored.')
    parser1.set_defaults(func=swap_cli)

    args = parser.parse_args()

    return args


if __name__ == '__main__':
    fmt = '%(asctime)s [%(threadName)s] %(levelname)s %(lineno)d: - %(message)s'
    dfmt = '%Y-%m-%d %H:%M:%S'
    logging.basicConfig(format=fmt, datefmt=dfmt, level=logging.INFO)

    args = parseCliArgs()
    args.func(args)
