from collections import OrderedDict
import json
import logging
import os

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State


class PmmlFinalize(State):

    def __init__(self):
        State.__init__(self, "PmmlFinalize")
        self.logger = logging.getLogger(name='pmmlfinalize')
    
    @overrides(State)
    def execute(self):
        self.writeJson(self.getMediator())
        self.writeEnhancedFiles(self.getMediator())
        
    def writeJson(self, mediator):
        stateMachine = self.getStateMachine()
        states = stateMachine.getStates()

        jsonDict = OrderedDict()
        for state in states:
            if isinstance(state, JsonGenBase):
                key = state.getKey()
                value = state.getJsonProperty()
                jsonDict[key] = value

        with open(mediator.modelLocalDir + mediator.name + "_model.json", "wb") as fp:
            json.dump(jsonDict, fp)
            
    def writeEnhancedFiles(self, mediator):
        base = self.mediator.modelEnhancementsLocalDir
        
        with open(os.path.join(base, "modelsummary.json"), "wb") as f:
            json.dump(self.mediator.enhancedsummary, f, indent=4)
        
        with open(os.path.join(base, "datacomposition.json"), "wb") as f:
            json.dump(self.mediator.data_composition, f, indent=4)
            
