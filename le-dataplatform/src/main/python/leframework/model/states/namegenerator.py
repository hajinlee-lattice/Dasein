import logging
import time

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State


class NameGenerator(State, JsonGenBase):

    def __init__(self):
        State.__init__(self, "NameGenerator")
        self.logger = logging.getLogger(name = 'namegenerator')
    
    @overrides(State)
    def execute(self):
        modelPrefix = ""
        if "DataLoader_TenantName" in self.mediator.provenanceProperties and self.mediator.provenanceProperties["DataLoader_TenantName"] != None:
            modelPrefix = self.mediator.provenanceProperties["DataLoader_TenantName"] + "_"
        self.mediator.name = modelPrefix + self.mediator.schema["name"] + "_" + time.strftime("%Y-%m-%d_%H-%M")
        
    @overrides(JsonGenBase)
    def getKey(self):
        return "Name"
    
    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.mediator.name