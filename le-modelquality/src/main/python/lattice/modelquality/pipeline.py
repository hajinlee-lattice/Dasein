
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import copy
from .entitybase import EntityBase
from .pipelineresource import PipelineResource

class Pipeline(EntityBase):

    @classmethod
    def getAll(cls):
        pipelines = []
        pipelinedefs = PipelineResource().getAll()
        for pipelinedef in pipelinedefs:
            pipelines.append(cls.createFromConfig(pipelinedef))
        return pipelines

    @classmethod
    def getAllNames(cls):
        return PipelineResource().getAllNames()

    @classmethod
    def getByName(cls, name):
        pipelinedef = PipelineResource().getByName(name)
        return cls.createFromConfig(pipelinedef)

    @classmethod
    def createFromConfig(cls, config):
        pipeline = Pipeline(config['name'])
        pipeline.setPipelineDriver(config['pipeline_driver'])
        pipeline.setPipelineLibScript(config['pipeline_lib_script'])
        pipeline.setPipelineScript(config['pipeline_script'])
        pipeline.setPipelineSteps(config['pipeline_steps'])
        return pipeline

    def __init__(self, name):
        super(Pipeline, self).__init__('pipelines/')
        self._config['name'] = name
        self._config['pipeline_driver'] = ''
        self._config['pipeline_lib_script'] = ''
        self._config['pipeline_script'] = ''
        self._config['pipeline_steps'] = []

    def setName(self, name):
        self._config['name'] = name

    def getName(self):
        return self._config['name']

    def setPipelineDriver(self, pipeline_driver):
        self._config['pipeline_driver'] = pipeline_driver

    def getPipelineDriver(self):
        return self._config['pipeline_driver']

    def setPipelineLibScript(self, pipeline_lib_script):
        self._config['pipeline_lib_script'] = pipeline_lib_script

    def getPipelineLibScript(self):
        return self._config['pipeline_lib_script']

    def setPipelineScript(self, pipeline_script):
        self._config['pipeline_script'] = pipeline_script

    def getPipelineScript(self):
        return self._config['pipeline_script']

    def setPipelineSteps(self, pipeline_steps):
        self._config['pipeline_steps'] = copy.deepcopy(pipeline_steps)

    def getPipelineSteps(self):
        return copy.deepcopy(self._config['pipeline_steps'])

    def getStepNamesInOrder(self):
        return [s['Name'] for s in self._config['pipeline_steps']]
