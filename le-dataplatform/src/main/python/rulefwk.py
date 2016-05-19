from abc import abstractmethod
from pipelinefwk import Pipeline
from pipelinefwk import PipelineStep

class DataRulePipeline(Pipeline):

    def __init__(self):
        super(DataRulePipeline, self).__init__()

    def apply(self, dataFrame, configMetadata):
        for step in self.pipelineSteps:
            step.apply(dataFrame, configMetadata)
        return dataFrame

    def processResults(self):
        for step in self.pipelineSteps:
            if isinstance(step, ColumnRule):
                step.getColumnsToRemove()
            elif isinstance(step, RowRule):
                step.getRowsToRemove()
            elif isinstance(step, TableRule):
                step.getRowsToRemove()

class DataRule(PipelineStep):

    def __init__(self):
        super(DataRule, self).__init__()

    @abstractmethod
    def apply(self, dataFrame, configMetadata):
        return

    @abstractmethod
    def getDescription(self):
        return

class RowRule(DataRule):

    def __init__(self):
        super(RowRule, self).__init__()

    @abstractmethod
    def getRowsToRemove(self):
        return

class ColumnRule(DataRule):

    def __init__(self):
        super(ColumnRule, self).__init__()

    @abstractmethod
    def getColumnsToRemove(self):
        return

class TableRule(DataRule):

    def __init__(self):
        super(TableRule, self).__init__()

    @abstractmethod
    def getRowsToRemove(self):
        return
