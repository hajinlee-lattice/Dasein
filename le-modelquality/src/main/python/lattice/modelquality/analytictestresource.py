
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

class AnalyticTestResource(EntityResource):

    def __init__(self):
        super(AnalyticTestResource, self).__init__('analytictests/')

    def createForProduction(self):
        raise RuntimeError('There is no standard production AnalyticTest defined')

    def execute(self, name):
        url = self._url + 'execute/' + name
        response = requests.put(url, headers=self.header_put, verify=EnvConfig().doVerify())
        if response.status_code != 200:
            raise RuntimeError('HTTP GET request failed for resource \"{0}\" with code {1}: {2}'.format(self._resource, response.status_code, response.text))
        try:
            return response.json()
        except:
            raise RuntimeError('Entity with name \"{}\" does not exist'.format(name))
