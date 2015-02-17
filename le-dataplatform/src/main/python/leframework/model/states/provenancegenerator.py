from collections import OrderedDict
import logging

from leframework.codestyle import overrides
from leframework.model.state import State

class ProvenanceGenerator(State):

    def __init__(self):
        State.__init__(self, "ProvenanceGenerator")
        self.logger = logging.getLogger(name='provenancegenerator')

    @overrides(State)
    def execute(self):
        properties = self.mediator.provenanceProperties

        if len(properties) > 0:
            self.result = OrderedDict()
            self.result["SourceURL"] = properties["DataLoader_Instance"]
            self.result["TenantName"] = properties["DataLoader_TenantName"]
            self.result["QueryName"] = properties["DataLoader_Query"]
        else:
            self.logger.error("Provenance property is null.")
            self.result = OrderedDict()

        # Add Result to Mediator
        self.mediator.eventtableprovenance = self.result
