
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

try:
    import requests_toolbelt
except ImportError as ie:
    print ''
    print 'Module \'requests_toolbelt\' must be installed.'
    print ''
    raise ie

import re, os
from .envconfig import EnvConfig
from .entityresource import EntityResource

class PipelineResource(EntityResource):

    def __init__(self):
        super(PipelineResource, self).__init__('pipelines/')

    def uploadStepMetadata(self, pathname, jsonfilename, stepname):
        with open(os.path.join(pathname, jsonfilename), mode='rb') as jsonfile:
            url = self._url + 'pipelinestepfiles/metadata?fileName={0}&stepName={1}'.format(jsonfilename, stepname)
            m = requests_toolbelt.MultipartEncoder(fields={'file':(jsonfilename, jsonfile, 'text/plain')})
            response = requests.post(url, data=m, headers={'Content-Type': m.content_type}, verify=EnvConfig().doVerify())
            if response.status_code != 200:
                raise RuntimeError('HTTP POST request failed for resource \"{0}\" with code {1}: {2}'.format(self._resource, response.status_code, response.json()['errorMsg']))
            return response.text

    def uploadStepImplementation(self, pathname, pythonfilename, stepname):
        with open(os.path.join(pathname, pythonfilename), mode='rb') as pythonfile:
            url = self._url + 'pipelinestepfiles/python?fileName={0}&stepName={1}'.format(pythonfilename, stepname)
            m = requests_toolbelt.MultipartEncoder(fields={'file':(pythonfilename, pythonfile, 'text/plain')})
            response = requests.post(url, data=m, headers={'Content-Type': m.content_type}, verify=EnvConfig().doVerify())
            if response.status_code != 200:
                raise RuntimeError('HTTP POST request failed for resource \"{0}\" with code {1}: {2}'.format(self._resource, response.status_code, response.json()['errorMsg']))
            return response.text
