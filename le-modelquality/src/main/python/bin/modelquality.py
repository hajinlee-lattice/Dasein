#!/usr/bin/env python

import argparse, datetime, json, sys, os
sys.path.append(os.path.join(os.path.dirname(__file__),'..'))

from lattice.modelquality import *

ENTITIES = { \
    'algorithm' : Algorithm, \
    'dataflow' : Dataflow, \
    'pipeline' : Pipeline, \
    'propdata' : PropData, \
    'sampling' : Sampling, \
    'dataset' : Dataset \
}

def workspace_initialize(name, tenant, username, password):
    
    env = EnvConfig().getName()
    pathname = os.path.join('modelquality-'+env,name)

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

    with open('.modelquality_workspace', mode='wb') as config:
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

def list(entitycls):

    for name in entitycls.getAllNames():
        print ' * {}'.format(name)

def printentity(entitycls, name):

    entity = entitycls.getByName(name)
    entity.printConfig()


    exit(0)

def createnew(entitytype, entitycls, name):

    entity = entitycls(name)
    _write_entity(entitytype, entity)

def install(entitytype, entitycls, name):

    env = EnvConfig().getName()
    name = name + '_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    
    entity = _read_entity(entitytype, entitycls)
    entity.setName(name)
    _write_entity(entitytype, entity)
    entity.install()

def model(name, description):

    name = name + '_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

    algorithm = _read_entity('algorithm', Algorithm)
    dataflow = _read_entity('dataflow', Dataflow)
    pipeline = _read_entity('pipeline', Pipeline)
    propdata = _read_entity('propdata', PropData)
    sampling = _read_entity('sampling', Sampling)
    dataset = _read_entity('dataset', Dataset)

    workspace = ''
    with open('.modelquality_workspace', mode='rb') as workspace_config:
        workspace = workspace_config.read()

    env = EnvConfig().getName()
    
    modelconfig = {}
    with open(os.path.join('modelquality-'+env, workspace, 'modelconfig.json')) as modelconfigfile:
        modelconfig = json.loads(modelconfigfile.read())

    tenant = modelconfig['tenant']
    username = modelconfig['username']
    password = modelconfig['password']

    print ModelRun.run(name, description, algorithm, dataflow, pipeline, propdata, sampling, dataset, \
        tenant, username, password)

    #print 'All Model Runs'
    #print ModelRun.getAll()

def _write_entity(entitytype, entity):

    config = entity.getConfig()
    workspace = ''
    try:
        with open('.modelquality_workspace', mode='rb') as workspace_config:
            workspace = workspace_config.read()
    except IOError:
        print 'No workspace set; run \"init-workspace\"'
        return

    env = EnvConfig().getName()
    entitypath = os.path.join('modelquality-'+env, workspace, entitytype+'.json')
    try:
        with open(entitypath, mode='wb') as outfile:
            outfile.write(json.dumps(config, indent=4))
    except IOError:
        print 'No workspace exists; run \"init-workspace\"'
        return

def _read_entity(entitytype, entitycls):

    workspace = ''
    try:
        with open('.modelquality_workspace', mode='rb') as workspace_config:
            workspace = workspace_config.read()
    except IOError:
        print 'No workspace set; run \"init-workspace\"'
        return None

    env = EnvConfig().getName()
    entitypath = os.path.join('modelquality-'+env, workspace, entitytype+'.json')
    config = '{}'
    try:
        with open(entitypath, mode='rb') as infile:
            config = json.loads(infile.read())
    except IOError:
        print 'No workspace exists; run \"init-workspace\"'
        return None

    return entitycls.createFromConfig(config)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Command-line interface to the Lattice model quality framework')
    parser.add_argument('-e', '--environment', metavar='ENV', choices=['devel','qa'], default='devel')
    parser.add_argument('-v', '--verbose', action='store_true')

    subparsers = parser.add_subparsers(dest='subcommand', title='available commands', metavar='command <arguments> ...')

    parser_initworkspace = subparsers.add_parser('init-workspace', help='initialize a local workspace for working with .json configuration files')
    parser_initworkspace.add_argument('name')
    parser_initworkspace.add_argument('tenant')
    parser_initworkspace.add_argument('username')
    parser_initworkspace.add_argument('password')

    parser_useworkspace = subparsers.add_parser('use-workspace')
    parser_useworkspace.add_argument('name')

    parser_retrieve = subparsers.add_parser('retrieve')
    parser_retrieve.add_argument('module', choices=['algorithm', 'dataflow', 'pipeline', 'propdata', 'sampling', 'dataset'])
    parser_retrieve.add_argument('name')

    parser_install_latest = subparsers.add_parser('install-latest')

    parser_list = subparsers.add_parser('list')
    parser_list.add_argument('module', choices=['algorithm', 'dataflow', 'pipeline', 'propdata', 'sampling', 'dataset'])

    parser_print = subparsers.add_parser('print')
    parser_print.add_argument('module', choices=['algorithm', 'dataflow', 'pipeline', 'propdata', 'sampling', 'dataset'])
    parser_print.add_argument('name')

    parser_print = subparsers.add_parser('new')
    parser_print.add_argument('module', choices=['algorithm', 'dataflow', 'pipeline', 'propdata', 'sampling', 'dataset'])
    parser_print.add_argument('name')

    parser_print = subparsers.add_parser('model')
    parser_print.add_argument('name')
    parser_print.add_argument('description')

    parser_retrieve = subparsers.add_parser('install')
    parser_retrieve.add_argument('module', choices=['algorithm', 'dataflow', 'pipeline', 'propdata', 'sampling', 'dataset'])
    parser_retrieve.add_argument('name')
    
    args = parser.parse_args()

    EnvConfig(args.environment, verbose=args.verbose)

    if args.subcommand == 'init-workspace':
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
        model(args.name, args.description)
    elif args.subcommand == 'install':
        install(args.module, ENTITIES[args.module], args.name)
