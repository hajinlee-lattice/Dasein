from collections import OrderedDict
from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State
import logging

class PmmlSummaryGenerator(State, JsonGenBase):

    def __init__(self):
        State.__init__(self, "PmmlSummaryGenerator")
        self.logger = logging.getLogger(name='pmmlsummarygenerator')

    @overrides(State)
    def execute(self):
        mediator = self.mediator
        self.summary = OrderedDict()
        self.summary["SchemaVersion"] = 1
        self.summary["ModelID"] = mediator.modelId

    @overrides(JsonGenBase)
    def getKey(self):
        return "Summary"

    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.summary
