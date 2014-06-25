from collections import OrderedDict
import logging
import pickle

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State
from pipeline import ModelStep

class ModelGenerator(State, JsonGenBase):
    
    def __init__(self):
        State.__init__(self, "ModelGenerator")
        self.logger = logging.getLogger(name = 'modelgenerator')
        
    
    @overrides(State)
    def execute(self):
        mediator = self.mediator
        filename = mediator.modelLocalDir + '/STPipelineBinary.p'
        pickle.dump(mediator.clf, open(filename, "w"), pickle.HIGHEST_PROTOCOL)
        model = OrderedDict()
        model["__type"] = "PythonScriptModel:#LatticeEngines.DataBroker.ServiceInterface"
        model["AdjustmentFactor"] = 1
        model["ColumnMetadata"] = None
        model["InitialTransforms"] = None
        model["Target"] = 1
        with open("leframework.tar.gz/leframework/scoringengine.py", "r") as pythonFile:
            model["Script"] = "".join(pythonFile.readlines())

        mediator.pipeline.getPipeline().append(ModelStep(self.mediator.clf, self.mediator.schema["features"]))

        pipeline = mediator.pipeline
        pickle.dump(pipeline, open(filename, "w"), pickle.HIGHEST_PROTOCOL)
        
        pipelineBinaryPkl = self.__getSerializedFile(filename)
        pipelinePkl = self.__getSerializedFile("leframework.tar.gz/pipeline.py")
        encoderPkl = self.__getSerializedFile("leframework.tar.gz/encoder.py")
        model["SupportFiles"] = [{"Value": encoderPkl, "Key": "encoder.py" }, {"Value": pipelinePkl, "Key": "pipeline.py" }, {"Value": pipelineBinaryPkl, "Key": "STPipelineBinary.p" }]
        self.model = model
        
    def __getSerializedFile(self, filename):
        return map(lambda x: int(x), bytearray(open(filename, "rb").read()))
    
    @overrides(JsonGenBase)
    def getKey(self):
        return "Model"
    
    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.model
