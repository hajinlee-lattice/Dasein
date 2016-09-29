
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import json, os, sys

class EnvConfig(object):

    environments = ['qa','devel']

    _initialized = False
    _envname = ''
    _endpoint = ''
    _apiHostPort = ''
    _influxEndpoint = ''
    _hdfsHostPort = ''
    _hdfsBasePath = ''
    _verify = ''
    _verbose = ''

    def __init__(self, env='qa', verbose=False):
        if not EnvConfig._initialized:
            if env.lower() not in self.environments:
                raise ReferenceError('Unknown environment \"{0}\"; must be one of {1}'.format(env, str(self.environments)))
            EnvConfig._initialized = True
            EnvConfig._envname = env
            with open(os.path.join(os.path.dirname(__file__),'conf') + '/{}.json'.format(env.lower()), mode='rb') as cfgfile:
                cfg = json.loads(cfgfile.read())
                EnvConfig._endpoint = cfg['endpoint']
                EnvConfig._apiHostPort = cfg['apiHostPort']
                EnvConfig._influxEndpoint = cfg['influxEndpoint']
                EnvConfig._hdfsHostPort = cfg['hdfsHostPort']
                EnvConfig._hdfsBasePath = cfg['hdfsBasePath']
                EnvConfig._verify = cfg['verify']
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
    def getHDFSHostPort(self):
        return EnvConfig._hdfsHostPort

    @classmethod
    def getHDFSBasePath(self):
        return EnvConfig._hdfsBasePath

    @classmethod
    def isVerbose(self):
        return EnvConfig._verbose

    @classmethod
    def doVerify(self):
        return EnvConfig._verify
