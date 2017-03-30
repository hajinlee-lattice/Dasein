
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

import re, os
from .envconfig import EnvConfig
from .entityresource import EntityResource

class ModelRunResource(EntityResource):

    def __init__(self):
        super(ModelRunResource, self).__init__('modelruns/')

    def getStatus(self, name):
        url = self._url + 'status/' + name
        response = requests.get(url, headers=self.header_get, verify=EnvConfig().doVerify())
        if response.status_code != 200:
            raise RuntimeError('HTTP GET request failed for resource \"{0}\" with code {1}: {2}'.format(self._resource, response.status_code, response.json()['errorMsg']))
        try:
            return response.text
        except:
            raise RuntimeError('Entity with name \"{}\" does not exist'.format(name))

    def getHDFSDir(self, name):
        url = self._url + 'modelhdfsdir/' + name
        response = requests.get(url, headers=self.header_get, verify=EnvConfig().doVerify())
        if response.status_code != 200:
            raise RuntimeError('HTTP GET request failed for resource \"{0}\" with code {1}: {2}'.format(self._resource, response.status_code, response.json()['errorMsg']))
        try:
            return response.text
        except:
            raise RuntimeError('Entity with name \"{}\" does not exist'.format(name))

    def create(self, dfpars, tenant, username, password):

        apiHostPort = EnvConfig().getApiHostPort()
        url = self._url + '?tenant={0}&username={1}&password={2}&apiHostPort={3}'.format(tenant, username, password, apiHostPort)
        response = requests.post(url, json=dfpars, headers=self.header_post, verify=EnvConfig().doVerify())
        if response.status_code != 200:
            raise RuntimeError('HTTP POST request failed for resource \"{0}\" with code {1}: {2}'.format(self._resource, response.status_code, response.json()['errorMsg']))
        return response.text

    def createForProduction(self):
        raise RuntimeError('There is no standard production ModelRun defined')
