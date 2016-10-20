
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

try:
    import requests
    from requests.packages import urllib3
    urllib3.disable_warnings()
except ImportError as ie:
    print ''
    print 'Module \'requests\' must be installed.'
    print ''
    raise ie

import json, os, sys

class EnvConfig(object):

    environments = ['qa','devel','prod']

    _initialized = False
    _envname = ''
    _endpoint = ''
    _apiHostPort = ''
    _influxEndpoint = ''
    _hdfsHostPortList = []
    _hdfsBasePath = ''
    _verify = ''
    _verbose = ''

    def __init__(self, env='prod', verbose=False):
        if not EnvConfig._initialized:
            if env.lower() not in self.environments:
                raise ReferenceError('Unknown environment \"{0}\"; must be one of {1}'.format(env, str(self.environments)))
            EnvConfig._initialized = True
            EnvConfig._envname = env
            with open(os.path.join(os.path.dirname(__file__),'conf') + '/{}.json'.format(env.lower()), mode='rb') as cfgfile:
                cfg = json.loads(cfgfile.read())
                if env in ['qa', 'prod']:
                    response = requests.get(cfg['currentStackEndpoint'], verify=False)
                    stack = response.json()['CurrentStack']
                    EnvConfig._endpoint = cfg['endpoint'][stack]
                    EnvConfig._apiHostPort = cfg['apiHostPort'][stack]
                else:
                    EnvConfig._endpoint = cfg['endpoint']
                    EnvConfig._apiHostPort = cfg['apiHostPort']
                EnvConfig._influxEndpoint = cfg['influxEndpoint']
                EnvConfig._hdfsHostPortList = cfg['hdfsHostPortList']
                EnvConfig._hdfsBasePath = cfg['hdfsBasePath']
                EnvConfig._verify = False if cfg['verify'] == 'False' else True
                EnvConfig._verbose = verbose

    @classmethod
    def getName(self):
        return EnvConfig._envname

    @classmethod
    def getEndpoint(self):
        return EnvConfig._endpoint

    @classmethod
    def getApiHostPort(self):
        return EnvConfig._apiHostPort

    @classmethod
    def getInfluxEndpoint(self):
        return EnvConfig._influxEndpoint

    @classmethod
    def getHDFSHostPortList(self):
        return EnvConfig._hdfsHostPortList

    @classmethod
    def getHDFSBasePath(self):
        return EnvConfig._hdfsBasePath

    @classmethod
    def isVerbose(self):
        return EnvConfig._verbose

    @classmethod
    def doVerify(self):
        return EnvConfig._verify
