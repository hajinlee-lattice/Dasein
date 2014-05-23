from leframework.model import jsongenbase as jb
from leframework.model import state
from leframework import codestyle as cs

class SummaryGenerator(state.State, jb.JsonGenBase):
    
    def __init__(self):
        state.State.__init__(self, "SummaryGenerator")
    
    @cs.overrides(state.State)
    def execute(self):
        summary = dict()
        predictors = []
        summary["Predictors"] = predictors
    
    @cs.overrides(jb.JsonGenBase)
    def getKey(self):
        return "Summary"
    
    @cs.overrides(jb.JsonGenBase)
    def getJsonProperty(self):
        return self.summmary