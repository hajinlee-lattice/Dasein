from collections import OrderedDict
import logging

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State


class CalibrationGenerator(State, JsonGenBase):

    def __init__(self):
        State.__init__(self, "CalibrationGenerator")
        self.logger = logging.getLogger(name='calibrationgenerator')
    
    @overrides(State)
    def execute(self):
        calibration = []
        mediator = self.mediator
        scored = mediator.scored
        
        element = OrderedDict()
        element["MaximumScore"] = None
        
        calibration.append(element)
        self.calibration = calibration
    
    @overrides(JsonGenBase)
    def getKey(self):
        return "Calibration"
    
    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.calibration
