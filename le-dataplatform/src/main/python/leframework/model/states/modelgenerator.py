from sklearn.externals import joblib

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State


class ModelGenerator(State, JsonGenBase):
    
    def __init__(self):
        State.__init__(self, "ModelGenerator")
    
    @overrides(State)
    def execute(self):
        joblib.dump(self.mediator.clf, self.mediator.modelLocalDir + '/model.pkl', compress = 9)
        self.model = dict()
        self.model["__type"] = "PythonScriptModel:#LatticeEngines.DataBroker.ServiceInterface"
    
    @overrides(JsonGenBase)
    def getKey(self):
        return "Model"
    
    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.model