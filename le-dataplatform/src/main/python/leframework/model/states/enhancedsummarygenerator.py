from collections import OrderedDict
import logging

from leframework.codestyle import overrides
from leframework.model.state import State

class EnhancedSummaryGenerator(State):

    def __init__(self):
        State.__init__(self, "EnhancedSummaryGenerator")
        self.logger = logging.getLogger(name='enhancedsummarygenerator')

    @overrides(State)
    def execute(self):
        self.result = OrderedDict()

        self.result["Status"] = "Todo"

        # Add Result to Mediator
        self.mediator.enhancedsummary = self.result
