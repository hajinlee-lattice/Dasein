
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import copy
from .entitybase import EntityBase
from .pipelineresource import PipelineResource
from .pipelinestep import PipelineStep

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
        pipeline.setPipelineStepsFromDict(config['pipeline_steps'])
        return pipeline

    def __init__(self, name):
        super(Pipeline, self).__init__('pipelines/')
        self._config['name'] = name
        self._config['pipeline_driver'] = ''
        self._config['pipeline_lib_script'] = ''
        self._config['pipeline_script'] = ''
        self._config['pipeline_steps'] = []
        self._piplineSteps = []

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

    def setPipelineStepsFromDict(self, pipeline_steps):
        self._config['pipeline_steps'] = copy.deepcopy(pipeline_steps)
        self._piplineSteps = []
        for stepconfig in pipeline_steps:
            self._piplineSteps.append(PipelineStep.createFromConfig(stepconfig))

    def setPipelineSteps(self, pipelineSteps):
        self._piplineSteps = copy.deepcopy(pipelineSteps)
        self._config['pipeline_steps'] = []
        for step in pipelineSteps:
            self._config['pipeline_steps'].append(step.getConfig())

    def getPipelineSteps(self):
        return copy.deepcopy(self._piplineSteps)

    def getStepNamesInOrder(self):
        return [s['Name'] for s in self._config['pipeline_steps']]

    def install(self):
        stepOrFile = []
        for step in self.getPipelineSteps():
            step.install()
            if step.getHDFSPath() != '':
                stepOrFile.append({'pipeline_step_dir':step.getHDFSPath()})
            else:
                stepOrFile.append({'pipeline_step':step.getName()})
        return PipelineResource().create(self.getName(), stepOrFile)
