from leframework.model import jsongenbase as jb
from leframework.model import state
from sklearn.externals import joblib


class ModelGenerator(state.State, jb.JsonGenBase):
    
    def __init__(self):
        state.State.__init__(self, "ModelGenerator")
    
    def execute(self):
        joblib.dump(self.mediator.clf, self.mediator.modelLocalDir + '/model.pkl', compress = 9)
        self.model = dict()
        self.model["__type"] = "PythonScriptModel:#LatticeEngines.DataBroker.ServiceInterface"
    
    def getKey(self):
        return "Model"
    
    def getJsonProperty(self):
        return self.model