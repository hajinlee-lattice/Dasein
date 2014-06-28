import logging

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State

class NameGenerator(State, JsonGenBase):

    def __init__(self):
        State.__init__(self, "NameGenerator")
        self.logger = logging.getLogger(name = 'NameGenerator')
    
    @overrides(State)
    def execute(self):
        self.name = self.mediator.schema["name"]       
        
    @overrides(JsonGenBase)
    def getKey(self):
        return "Name"
    
    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.name