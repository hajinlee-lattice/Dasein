import logging

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State


class AverageProbabilityGenerator(State, JsonGenBase):

    def __init__(self):
        State.__init__(self, "AverageProbabilityGenerator")
        self.logger = logging.getLogger(name='averageprobabilitygenerator')

    @overrides(State)
    def execute(self):
        nrows = float(self.mediator.allDataPreTransform.shape[0])
        nevents = self.mediator.allDataPreTransform[self.mediator.schema["target"]].sum()
        self.averageProbability = nevents/nrows

    @overrides(JsonGenBase)
    def getKey(self):
        return "AverageProbability"

    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.averageProbability
