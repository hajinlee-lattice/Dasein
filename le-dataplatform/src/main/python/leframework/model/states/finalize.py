from collections import OrderedDict
import json
import logging
import numpy

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State


class Finalize(State):

    def __init__(self):
        State.__init__(self, "Finalize")
        self.logger = logging.getLogger(name='finalize')
    
    @overrides(State)
    def execute(self):
        self.writeScoredText(self.getMediator())
        self.writeJson(self.getMediator())
        
    def writeScoredText(self, mediator):
        scored = mediator.scored
        # add the key data and append the scored data
        keyData = mediator.data[:, mediator.schema["keyColIndex"]]
        scored = zip(keyData, scored)
        # write the scored data to file
        numpy.savetxt(mediator.modelLocalDir + "scored.txt", scored, delimiter=",")
        
    def writeJson(self, mediator):
        stateMachine = self.getStateMachine()
        states = stateMachine.getStates()
        jsonDict = OrderedDict()
        for state in states:
            if isinstance(state, JsonGenBase):
                key = state.getKey()
                value = state.getJsonProperty()
                jsonDict[key] = value
        with open(mediator.modelLocalDir + "model.json", "wb") as fp:
            json.dump(jsonDict, fp)

