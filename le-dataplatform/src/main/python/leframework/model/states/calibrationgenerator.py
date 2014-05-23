from leframework.model import state
from leframework.model import jsongenbase as jb

class CalibrationGenerator(state.State, jb.JsonGenBase):

    def __init__(self):
        state.State.__init__(self, "CalibrationGenerator")
    
    def execute(self):
        self.calibration = dict() 
    
    def getKey(self):
        return "Calibration"
    
    def getJsonProperty(self):
        return self.calibration
