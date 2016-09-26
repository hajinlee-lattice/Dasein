
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

import re
from .envconfig import EnvConfig
from .entityresource import EntityResource

class PipelineResource(EntityResource):

    def __init__(self):
        super(PipelineResource, self).__init__('pipelines/')

    def uploadStepMetadata(self, jsonfilename, stepname):
        url = self._url + 'pipelinestepfiles/metadata'
        response = requests.post(url, headers=self.header_get, verify=EnvConfig().doVerify())
        if response.status_code != 200:
            raise RuntimeError('HTTP GET request failed for resource \"{0}\" with code {1}: {2}'.format(self._resource, response.status_code, response.json()['errorMsg']))
        try:
            return response.json()
        except:
            raise RuntimeError('Entity with name \"{}\" does not exist'.format(name))

    def uploadStepImplementation(self, filename, stepname, file):
        url = self._url + name
        response = requests.get(url, headers=self.header_get, verify=EnvConfig().doVerify())
        if response.status_code != 200:
            raise RuntimeError('HTTP GET request failed for resource \"{0}\" with code {1}: {2}'.format(self._resource, response.status_code, response.json()['errorMsg']))
        try:
            return response.json()
        except:
            raise RuntimeError('Entity with name \"{}\" does not exist'.format(name))
