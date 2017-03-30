
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

class EntityResource(object):

    header_get = {"Accept": "application/json"}
    header_put = {"Content-Type": "application/json", "Accept": "application/json"}
    header_post = {"Content-Type": "application/json", "Accept": "application/json"}

    def __init__(self, resource):
        self._resource = ''
        if re.search('dataflow', resource.lower()):
            self._resource = 'dataflows/'
        elif re.search('dataset', resource.lower()):
            self._resource = 'datasets/'
        elif re.search('propdata', resource.lower()):
            self._resource = 'propdataconfigs/'
        elif re.search('sampling', resource.lower()):
            self._resource = 'samplingconfigs/'
        elif re.search('algorithm', resource.lower()):
            self._resource = 'algorithms/'
        elif re.search('analyticpipeline', resource.lower()):
            self._resource = 'analyticpipelines/'
        elif re.search('pipeline', resource.lower()):
            self._resource = 'pipelines/'
        elif re.search('modelrun', resource.lower()):
            self._resource = 'modelruns/'
        elif re.search('analytictest', resource.lower()):
            self._resource = 'analytictests/'
        self._url = EnvConfig().getEndpoint() + '/' + self._resource

    def getAll(self):
        response = requests.get(self._url, headers=self.header_get, verify=EnvConfig().doVerify())
        if response.status_code != 200:
            raise RuntimeError('HTTP GET request failed for resource \"{0}\" with code {1}: {2}'.format(self._resource, response.status_code, response.json()['errorMsg']))
        return response.json()

    def getAllNames(self):
        return sorted([df['name'] for df in self.getAll()])

    def getByName(self, name):
        url = self._url + name
        response = requests.get(url, headers=self.header_get, verify=EnvConfig().doVerify())
        if response.status_code != 200:
            raise RuntimeError('HTTP GET request failed for resource \"{0}\" with code {1}: {2}'.format(self._resource, response.status_code, response.text))
        try:
            return response.json()
        except:
            raise RuntimeError('Entity with name \"{}\" does not exist'.format(name))

    def create(self, dfpars):
        response = requests.post(self._url, json=dfpars, headers=self.header_post, verify=EnvConfig().doVerify())
        if response.status_code != 200:
            raise RuntimeError('HTTP POST request failed for resource \"{0}\" with code {1}: {2}'.format(self._resource, response.status_code, response.json()['errorMsg']))
        return response.text

    def createForProduction(self):
        url = self._url + 'latest'
        response = requests.post(url, headers=self.header_get, verify=EnvConfig().doVerify())
        if response.status_code != 200:
            raise RuntimeError('HTTP POST request failed for resource \"{0}\" with code {1}: {2}'.format(self._resource, response.status_code, response.json()['errorMsg']))
        return response.json()
