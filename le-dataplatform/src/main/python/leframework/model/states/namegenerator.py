import logging
import time
from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State

class NameGenerator(State, JsonGenBase):

    def __init__(self):
        State.__init__(self, "NameGenerator")
        self.logger = logging.getLogger(name = 'NameGenerator')
    
    @overrides(State)
    def execute(self):
        self.mediator.name = self.mediator.schema["name"] + "_" + time.strftime("%Y-%m-%d-%H:%M")
        
    @overrides(JsonGenBase)
    def getKey(self):
        return "Name"
    
    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.mediator.name