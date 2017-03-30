
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import argparse, datetime, json, shutil, sys, os

from algorithm import Algorithm
from dataflow import Dataflow
from pipeline import Pipeline
from propdata import PropData
from sampling import Sampling
from dataset import Dataset
from analyticpipeline import AnalyticPipeline
from analytictest import AnalyticTest
from modelrun import ModelRun
from envconfig import EnvConfig
from entityresource import EntityResource

ENTITIES = { \
    'algorithm' : Algorithm, \
    'dataflow' : Dataflow, \
    'pipeline' : Pipeline, \
    'propdata' : PropData, \
    'sampling' : Sampling, \
    'dataset' : Dataset, \
    'analyticpipeline' : AnalyticPipeline, \
    'analytictest' : AnalyticTest, \
    'modelrun' : ModelRun \
}

def main():
    parser = argparse.ArgumentParser(description='Command-line interface to the Lattice model quality framework')
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-e', '--env', metavar='ENV', choices=['devel', 'qa', 'prod'], default='prod', help='select environment (prod, qa, devel)')

    subparsers = parser.add_subparsers(dest='subcommand', title='available commands', metavar='command <arguments> ...')

    parser_getexamples = subparsers.add_parser('get-examples', help='copy a file with examples of using the package to the current directory')

    parser_initworkspace = subparsers.add_parser('init-workspace', help='initialize a local workspace for working with .json configuration files')
    parser_initworkspace.add_argument('name')
    parser_initworkspace.add_argument('tenant')
    parser_initworkspace.add_argument('username')
    parser_initworkspace.add_argument('password')

    parser_useworkspace = subparsers.add_parser('use-workspace', help='switch which local workspace is being used')
    parser_useworkspace.add_argument('name')

    parser_install_latest = subparsers.add_parser('install-latest', help='install the latest standard configurations to the model quality framework')

    parser_list = subparsers.add_parser('list', help='list all the configurations for a given entity')
    parser_list.add_argument('module', choices=['modelrun', 'analyticpipeline', 'algorithm', 'dataflow', 'pipeline', 'propdata', 'sampling', 'dataset', 'analytictest'])

    parser_print = subparsers.add_parser('print', help='print to STDOUT a named configuration for a given entity')
    parser_print.add_argument('module', choices=['modelrun', 'analyticpipeline', 'algorithm', 'dataflow', 'pipeline', 'propdata', 'sampling', 'dataset', 'analytictest'])
    parser_print.add_argument('name')

    parser_retrieve = subparsers.add_parser('retrieve', help='retrieve a named configuration for a given entity and write to the local workspace')
    parser_retrieve.add_argument('module', choices=['modelrun', 'analyticpipeline', 'algorithm', 'dataflow', 'pipeline', 'propdata', 'sampling', 'dataset', 'analytictest'])
    parser_retrieve.add_argument('name')

    parser_print = subparsers.add_parser('new', help='create a new named configuration for a given entity and write to the local workspace')
    parser_print.add_argument('module', choices=['modelrun', 'analyticpipeline', 'algorithm', 'dataflow', 'pipeline', 'propdata', 'sampling', 'dataset', 'analytictest'])
    parser_print.add_argument('name')

    parser_install = subparsers.add_parser('install', help='install the named configuration for a given entity')
    parser_install.add_argument('-t', '--timestamp', help='add a timestamp to the end of the name', action='store_true')
    parser_install.add_argument('module', choices=['analyticpipeline', 'algorithm', 'dataflow', 'pipeline', 'propdata', 'sampling', 'dataset', 'analytictest'])
    parser_install.add_argument('name')

    parser_model = subparsers.add_parser('model', help="submit a job to create a model (modelRun)")
    parser_model.add_argument('-t', '--timestamp', help='add a timestamp to the end of the name', action='store_true')
    parser_model.add_argument('name')
    parser_model.add_argument('description')

    parser_at = subparsers.add_parser('exectest', help="execute an analytic test")
    parser_at.add_argument('name')

    parser_modelstatus = subparsers.add_parser('model-status', help="get the status of a modelRun")
    parser_modelstatus.add_argument('name')

    parser_modelhdfsdir = subparsers.add_parser('model-hdfs-dir', help="get the HDFS directory for the output artifcats of a modelRun")
    parser_modelhdfsdir.add_argument('name')

    args = parser.parse_args()

    EnvConfig(args.env, verbose=args.verbose)

    if args.subcommand == 'get-examples':
        get_examples()
    elif args.subcommand == 'init-workspace':
        workspace_initialize(args.name, args.tenant, args.username, args.password)
    elif args.subcommand == 'use-workspace':
        workspace_use(args.name)
    elif args.subcommand == 'retrieve':
        retrieve(args.module, ENTITIES[args.module], args.name)
    elif args.subcommand == 'install-latest':
        install_latest()
    elif args.subcommand == 'list':
        list(ENTITIES[args.module])
    elif args.subcommand == 'print':
        printentity(ENTITIES[args.module], args.name)
    elif args.subcommand == 'new':
        createnew(args.module, ENTITIES[args.module], args.name)
    elif args.subcommand == 'model':
        model(args.name, args.description, args.timestamp)
    elif args.subcommand == 'install':
        install(args.module, ENTITIES[args.module], args.name, args.timestamp)
    elif args.subcommand == 'model-status':
        model_status(args.name)
    elif args.subcommand == 'model-hdfs-dir':
        model_hdfsdir(args.name)
    elif args.subcommand == 'exectest':
        execute_analytic_test(args.name)


def get_examples():
    shutil.copyfile(os.path.join(os.path.dirname(__file__), 'examples.py'), 'examples.py')

def workspace_initialize(name, tenant, username, password):

    env = EnvConfig().getName()
    pathname = os.path.join('modelquality-' + env, name)

    if not os.path.exists(pathname):
        os.makedirs(pathname)

    modelconfig = { \
        'tenant' : tenant, \
        'username' : username, \
        'password' : password \
    }

    with open(os.path.join(pathname, 'modelconfig.json'), mode='wb') as outfile:
        outfile.write(json.dumps(modelconfig, indent=4))

    workspace_use(name)

def workspace_use(name):

    env = EnvConfig().getName()
    with open('.modelquality_{}_workspace'.format(env), mode='wb') as config:
        config.write(name)

def retrieve(entitytype, entitycls, name):

    entity = entitycls.getByName(name)
    _write_entity(entitytype, entity)

def install_latest():

    try:
        EntityResource('algorithm').createForProduction()
        if EnvConfig().isVerbose():
            print 'Installed latest standard Algorithm'
    except:
        if EnvConfig().isVerbose():
            print 'Latest standard Algorithm already installed'

    try:
        EntityResource('dataflow').createForProduction()
        if EnvConfig().isVerbose():
            print 'Installed latest standard Dataflow'
    except:
        if EnvConfig().isVerbose():
            print 'Latest standard Dataflow already installed'

    try:
        EntityResource('pipeline').createForProduction()
        if EnvConfig().isVerbose():
            print 'Installed latest standard Pipeline'
    except:
        if EnvConfig().isVerbose():
            print 'Latest standard Pipeline already installed'

    try:
        EntityResource('propdata').createForProduction()
        if EnvConfig().isVerbose():
            print 'Installed latest standard PropData'
    except:
        if EnvConfig().isVerbose():
            print 'Latest standard PropData already installed'

    try:
        EntityResource('sampling').createForProduction()
        if EnvConfig().isVerbose():
            print 'Installed latest standard Sampling'
    except:
        if EnvConfig().isVerbose():
            print 'Latest standard Sampling already installed'

    try:
        EntityResource('analyticpipeline').createForProduction()
        if EnvConfig().isVerbose():
            print 'Installed latest standard AnalyticPipeline'
    except:
        if EnvConfig().isVerbose():
            print 'Latest standard AnalyticPipeline already installed'

def list(entitycls):

    for name in entitycls.getAllNames():
        print ' * {}'.format(name)

def printentity(entitycls, name):

    entity = entitycls.getByName(name)
    entity.printConfig()

def createnew(entitytype, entitycls, name):

    entity = entitycls(name)
    _write_entity(entitytype, entity)

def install(entitytype, entitycls, name, addTimestamp):

    env = EnvConfig().getName()
    if addTimestamp:
        name = name + '_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

    entity = _read_entity(entitytype, entitycls)
    entity.setName(name)
    _write_entity(entitytype, entity)
    entity.install()

def model(name, description, addTimestamp):

    if addTimestamp:
        name = name + '_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

    modelRun = _read_entity('modelrun', ModelRun)
    modelRun.setName(name)
    modelRun.setDescription(description)
    print 'Submitting modelRun:'
    modelRun.printConfig()

    env = EnvConfig().getName()

    workspace = ''
    with open('.modelquality_{}_workspace'.format(env), mode='rb') as workspace_config:
        workspace = workspace_config.read()

    modelconfig = {}
    with open(os.path.join('modelquality-' + env, workspace, 'modelconfig.json')) as modelconfigfile:
        modelconfig = json.loads(modelconfigfile.read())

    tenant = modelconfig['tenant']
    username = modelconfig['username']
    password = modelconfig['password']

    modelRun.execute(tenant, username, password)

def model_status(name):

    modelRun = ModelRun.getByName(name)
    print 'Status of model \"{0}\": {1}'.format(name, modelRun.getStatus())

def model_hdfsdir(name):

    modelRun = ModelRun.getByName(name)
    print 'HDFS directory for model \"{0}\": {1}'.format(name, modelRun.getHDFSDir())

def execute_analytic_test(name):

    analyticTest = AnalyticTest.getByName(name)
    print 'Executing analytic test:'
    analyticTest.printConfig()
    analyticTest.execute()

def _write_entity(entitytype, entity):

    config = entity.getConfig()
    env = EnvConfig().getName()
    workspace = ''
    try:
        with open('.modelquality_{}_workspace'.format(env), mode='rb') as workspace_config:
            workspace = workspace_config.read()
    except IOError:
        print 'No workspace set; run \"init-workspace\"'
        return

    entitypath = os.path.join('modelquality-' + env, workspace, entitytype + '.json')
    try:
        with open(entitypath, mode='wb') as outfile:
            outfile.write(json.dumps(config, indent=4))
    except IOError:
        print 'No workspace exists; run \"init-workspace\"'
        return

def _read_entity(entitytype, entitycls):

    workspace = ''
    env = EnvConfig().getName()
    try:
        with open('.modelquality_{}_workspace'.format(env), mode='rb') as workspace_config:
            workspace = workspace_config.read()
    except IOError:
        print 'No workspace set; run \"init-workspace\"'
        return None

    entitypath = os.path.join('modelquality-' + env, workspace, entitytype + '.json')
    config = '{}'
    try:
        with open(entitypath, mode='rb') as infile:
            config = json.loads(infile.read())
    except IOError:
        print 'No workspace exists; run \"init-workspace\"'
        return None

    return entitycls.createFromConfig(config)
