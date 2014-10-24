from collections import OrderedDict
import json
import logging
import numpy
import sys
import subprocess
from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State


class Finalize(State):

    def __init__(self):
        State.__init__(self, "Finalize")
        self.logger = logging.getLogger(name='finalize')
    
    @overrides(State)
    def execute(self):
        self.writeJson(self.getMediator())
        self.writeScoredText(self.getMediator())
        self.modelPredictorsExtraction(self.getMediator())

    def writeScoredText(self, mediator):
        scored = mediator.scored
        # add the key data and append the scored data
        keyData = mediator.data[:, mediator.schema["keyColIndex"]].astype(str)
        eventData = mediator.data[:, mediator.schema["targetIndex"]].astype(str)
        scored = numpy.insert(keyData, len(keyData[0]), scored, axis=1)
        # write the scored data to file
        numpy.savetxt(mediator.modelLocalDir + "scored.txt", scored, delimiter=",", fmt="%s")
        # write the target data to file
        numpy.savetxt(mediator.modelLocalDir + "target.txt", eventData, delimiter=",", fmt="%s")
        
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
            
    def modelPredictorsExtraction(self, mediator):
        modelJSONFilePath = mediator.modelLocalDir + "model.json"
        csvFilePath = mediator.modelLocalDir + "model.csv"
        subprocess.call([sys.executable, 'modelpredictorextraction.py', modelJSONFilePath, csvFilePath])
