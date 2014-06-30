import base64
from collections import OrderedDict
import gzip
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

        model = OrderedDict()
        model["__type"] = "PythonScriptModel:#LatticeEngines.DataBroker.ServiceInterface"
        model["AdjustmentFactor"] = 1
        model["ColumnMetadata"] = None
        model["InitialTransforms"] = None
        model["Target"] = 1
        with open("leframework.tar.gz/leframework/scoringengine.py", "r") as pythonFile:
            model["Script"] = "".join(pythonFile.readlines())

        mediator.pipeline.getPipeline().append(ModelStep(self.mediator.clf, self.mediator.schema["features"]))

        filename = mediator.modelLocalDir + '/STPipelineBinary.p'
        pipeline = mediator.pipeline
        pickle.dump(pipeline, open(filename, "w"), pickle.HIGHEST_PROTOCOL)
        filename = self.__compressFile(filename)
        pipelineBinaryPkl = self.__getSerializedFile(filename)
        pipelinePkl = self.__getSerializedFile(self.__compressFile("leframework.tar.gz/pipeline.py"))
        encoderPkl = self.__getSerializedFile(self.__compressFile("leframework.tar.gz/encoder.py"))
        model["CompressedSupportFiles"] = [{ "Value": encoderPkl, "Key": "encoder.py" }, { "Value": pipelinePkl, "Key": "pipeline.py" }, { "Value": pipelineBinaryPkl, "Key": "STPipelineBinary.p" }]
        self.model = model
    
    def __compressFile(self, filename):
        with open(filename, "rb") as uncompressedFile:
            with gzip.open(filename + ".gz", "wb", compresslevel=9) as compressedFile:
                compressedFile.write(uncompressedFile.read())
        return compressedFile.name

        
    def __getSerializedFile(self, filename):
        return base64.b64encode(bytearray(open(filename, "rb").read()))
    
    @overrides(JsonGenBase)
    def getKey(self):
        return "Model"
    
    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.model
