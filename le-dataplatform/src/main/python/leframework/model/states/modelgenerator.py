from collections import OrderedDict
from sklearn.externals import joblib

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State


class ModelGenerator(State, JsonGenBase):
    
    def __init__(self):
        State.__init__(self, "ModelGenerator")
    
    @overrides(State)
    def execute(self):
        filename = self.mediator.modelLocalDir + '/model.pkl'
        joblib.dump(self.mediator.clf, filename, compress = 9)
        model = OrderedDict()
        model["__type"] = "PythonScriptModel:#LatticeEngines.DataBroker.ServiceInterface"
        model["AdjustmentFactor"] = 1
        model["ColumnMetadata"] = None
        model["InitialTransforms"] = None
        model["Target"] = 1
        pklByteArray = map(lambda x: int(x), bytearray(open(filename, "rb").read()))
        model["SupportFiles"] = [{"Value": pklByteArray, "Key": "model.pkl" }]
        self.model = model
    
    @overrides(JsonGenBase)
    def getKey(self):
        return "Model"
    
    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.model