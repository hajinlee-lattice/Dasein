from abc import abstractmethod
from pipelinefwk import Pipeline
from pipelinefwk import PipelineStep

class DataRulePipeline(Pipeline):
    
    def __init__(self):
        super(DataRulePipeline, self).__init__()

class DataRule(PipelineStep):
    
    def __init__(self):
        super(DataRule, self).__init__()
    
    @abstractmethod
    def getName(self):
        return
    
    @abstractmethod
    def getRequiredAttributes(self):
        return
    
    @abstractmethod
    def getRowsToRemove(self):
        return
