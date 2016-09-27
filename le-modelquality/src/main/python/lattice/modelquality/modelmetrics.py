
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

class ModelMetrics(object):

    @classmethod
    def getAll(cls):
        metrics = []
        url = EnvConfig().getInfluxEndpoint()
        payload = {}
        payload['db'] = 'ModelQuality'
        payload['q'] = 'SELECT ModelID,RocScore,Top10PercentLift,Top20PercentLift FROM ModelingMeasurement'
        response = requests.get(url, params=payload, verify=True)
        if response.status_code != 200:
            raise RuntimeError('HTTP GET request failed for InfluxDB with code {0}: {1}'.format(response.status_code, response.text))
        data = response.json()['results'][0]['series'][0]
        columns = data['columns']
        values = data['values']
        for value in values:
            config = {}
            config['pointtime'] = value[0]
            config['modelid'] = value[1]
            config['ROC'] = value[2]
            config['top10PercentLift'] = value[3]
            config['top20PercentLift'] = value[4]
            metrics.append(ModelMetrics.createFromConfig(config))
        return metrics

    @classmethod
    def getAllModelIDs(cls):
        return [metrics.getModelID() for metrics in cls.getAll()]

    @classmethod
    def getByModelID(cls, modelid):
        url = EnvConfig().getInfluxEndpoint()
        payload = {}
        payload['db'] = 'ModelQuality'
        payload['q'] = 'SELECT ModelID,RocScore,Top10PercentLift,Top20PercentLift FROM ModelingMeasurement WHERE ModelID=\'{}\''.format(modelid)
        response = requests.get(url, params=payload, verify=True)
        if response.status_code != 200:
            raise RuntimeError('HTTP GET request failed for InfluxDB with code {0}: {1}'.format(response.status_code, response.text))
        data = response.json()['results'][0]['series'][0]
        columns = data['columns']
        value = data['values'][0]
        config = {}
        config['pointtime'] = value[0]
        config['modelid'] = value[1]
        config['ROC'] = value[2]
        config['top10PercentLift'] = value[3]
        config['top20PercentLift'] = value[4]
        return ModelMetrics.createFromConfig(config)

    @classmethod
    def createFromConfig(cls, config):
        modelmetrics = ModelMetrics(config['modelid'])
        modelmetrics.setTime(config['pointtime'])
        modelmetrics.setROC(config['ROC'])
        modelmetrics.setTop10PercentLift(config['top10PercentLift'])
        modelmetrics.setTop20PercentLift(config['top20PercentLift'])
        return modelmetrics

    def __init__(self, modelid):
        self._config = {}
        self._config['modelid'] = modelid
        self._config['pointtime'] = None
        self._config['ROC'] = 0.0
        self._config['top10PercentLift'] = 0.0
        self._config['top20PercentLift'] = 0.0

    def setModelID(self, modelid):
        self._config['modelid'] = modelid

    def getModelID(self):
        return self._config['modelid']

    def setTime(self, pointtime):
        self._config['pointtime'] = pointtime

    def getTime(self):
        return self._config['pointtime']

    def setROC(self, roc):
        self._config['roc'] = roc

    def getROC(self):
        return self._config['roc']

    def setTop10PercentLift(self, top10PercentLift):
        self._config['top10PercentLift'] = top10PercentLift

    def getTop10PercentLift(self):
        return self._config['top10PercentLift']

    def setTop20PercentLift(self, top20PercentLift):
        self._config['top20PercentLift'] = top20PercentLift

    def getTop20PercentLift(self):
        return self._config['top20PercentLift']
