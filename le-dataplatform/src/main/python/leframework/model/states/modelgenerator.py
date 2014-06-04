from collections import OrderedDict
import logging
import pickle
from sklearn.externals import joblib

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State
from leframework.pipeline import ModelStep
from leframework.pipeline import Pipeline


class ModelGenerator(State, JsonGenBase):
    
    def __init__(self):
        State.__init__(self, "ModelGenerator")
        self.logger = logging.getLogger(name='modelgenerator')
        
    
    @overrides(State)
    def execute(self):
        filename = self.mediator.modelLocalDir + '/STPipelineBinary.p'
        joblib.dump(self.mediator.clf, filename, compress=9)
        model = OrderedDict()
        model["__type"] = "PythonScriptModel:#LatticeEngines.DataBroker.ServiceInterface"
        model["AdjustmentFactor"] = 1
        model["ColumnMetadata"] = None
        model["InitialTransforms"] = None
        model["Target"] = 1
        with open("leframework.tar.gz/leframework/scoringengine.py", "r") as pythonFile:
            model["Script"] = "".join(pythonFile.readlines())
        modelSteps = [ ModelStep(self.mediator.clf, self.mediator.schema["features"]) ]
        pipeline = Pipeline(modelSteps)
        pickle.dump(pipeline, open(filename, "w"))
        
        pklByteArray = map(lambda x: int(x), bytearray(open(filename, "rb").read()))
        model["SupportFiles"] = [{"Value": pklByteArray, "Key": "STPipelineBinary.p" }]
        self.model = model
    
    @overrides(JsonGenBase)
    def getKey(self):
        return "Model"
    
    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.model