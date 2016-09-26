
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

from .envconfig import EnvConfig

class ModelRun(object):

    @classmethod
    def getAll(cls):
        url = EnvConfig().getEndpoint() + '/modelruns'
        header_get = {"Accept": "application/json"}
        response = requests.get(url, headers=header_get, verify=EnvConfig().doVerify())
        if response.status_code != 200:
            raise RuntimeError('HTTP GET request failed for resource \"modelruns\" with code {0}: {1}'.format(response.status_code, str(response.json()['Errors'])))
        return response.json()

    @classmethod
    def run(cls, name, description, algorithm, dataflow, pipeline, propdata, sampling, dataset, \
                tenant, username, password):
        
        selectedConfig = {}
        selectedConfig['algorithm'] = algorithm.getConfig();
        selectedConfig['data_flow'] = dataflow.getConfig();
        selectedConfig['data_set'] = dataset.getConfig();
        selectedConfig['pipeline'] = pipeline.getConfig();
        selectedConfig['prop_data'] = propdata.getConfig();
        selectedConfig['sampling'] = sampling.getConfig();
        
        body = {}
        body['name'] = name
        body['description'] = description
        body['selectedConfig'] = selectedConfig
        
        header_post = {"Content-Type": "application/json", "Accept": "application/json"}
        apiHostPort = EnvConfig().getApiHostPort()
        url = EnvConfig().getEndpoint() + '/modelruns' + '?tenant={0}&username={1}&password={2}&apiHostPort={3}'.format(tenant, username, password, apiHostPort)

        response = requests.post(url, json=body, headers=header_post, verify=EnvConfig().doVerify())
        if response.status_code != 200:
            raise RuntimeError('HTTP POST request failed for resource \"modelruns\" with code {0}: {1}'.format(response.status_code, str(response.json()['Errors'])))
        return response.json()
