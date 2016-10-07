
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from .entitybase import EntityBase
from .entityresource import EntityResource

class AnalyticPipeline(EntityBase):

    @classmethod
    def getAll(cls):
        aps = []
        apconfigs = EntityResource('analyticpipelines/').getAll()
        for apconfig in apconfigs:
            aps.append(cls.createFromConfig(apconfig))
        return aps

    @classmethod
    def getAllNames(cls):
        return EntityResource('analyticpipelines/').getAllNames()

    @classmethod
    def getByName(cls, name):
        apconfig = EntityResource('analyticpipelines/').getByName(name)
        return cls.createFromConfig(apconfig)

    @classmethod
    def createFromConfig(cls, config):
        ap = AnalyticPipeline(config['name'])
        ap.setAlgorithmName(config['algorithm_name'])
        ap.setDataflowName(config['dataflow_name'])
        ap.setPipelineName(config['pipeline_name'])
        ap.setPropDataName(config['prop_data_name'])
        ap.setSamplingName(config['sampling_name'])
        return ap

    def __init__(self, name):
        super(AnalyticPipeline, self).__init__('analyticpipelines/')
        self._config['name'] = name
        self._config['algorithm_name'] = ''
        self._config['dataflow_name'] = ''
        self._config['pipeline_name'] = ''
        self._config['prop_data_name'] = ''
        self._config['sampling_name'] = ''

    def setName(self, name):
        self._config['name'] = name

    def getName(self):
        return self._config['name']

    def setAlgorithmName(self, algorithm_name):
        self._config['algorithm_name'] = algorithm_name

    def getAlgorithmName(self):
        return self._config['algorithm_name']

    def setDataflowName(self, dataflow_name):
        self._config['dataflow_name'] = dataflow_name

    def getDataflowName(self):
        return self._config['dataflow_name']

    def setPipelineName(self, pipeline_name):
        self._config['pipeline_name'] = pipeline_name

    def getPipelineName(self):
        return self._config['pipeline_name']

    def setPropDataName(self, prop_data_name):
        self._config['prop_data_name'] = prop_data_name

    def getPropDataName(self):
        return self._config['prop_data_name']

    def setSamplingName(self, sampling_name):
        self._config['sampling_name'] = sampling_name

    def getSamplingName(self):
        return self._config['sampling_name']
