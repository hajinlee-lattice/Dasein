from collections import OrderedDict
import logging

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State
from leframework.util.modelutil import ModelUtil as mu


class PmmlModelSkeletonGenerator(State, JsonGenBase):
    
    def __init__(self):
        State.__init__(self, "PmmlModelSkeletonGenerator")
        self.logger = logging.getLogger(name = 'pmmlmodelskeletongenerator')
        
    
    @overrides(State)
    def execute(self):
        model = OrderedDict()
        model["__type"] = "PmmlModel:#LatticeEngines.DataBroker.ServiceInterface"
        model["AdjustmentFactor"] = 1
        model["ColumnMetadata"] = None
        model["InitialTransforms"] = None
        model["Target"] = 1
        version = self.__getLatticeVersion()
        if version is not None:
            model["LatticeVersion"] = version

        pipeline = mu.getPipeline(self.mediator)
        model["CompressedSupportFiles"] = []
        for step in pipeline.getPipeline():
            rtsArtifacts = step.getRTSArtifacts()
            
            if len(rtsArtifacts) == 0:
                continue
            for key, filePath in rtsArtifacts:
                if filePath is not None:
                    filePkl = mu.getSerializedFile(mu.compressFile(filePath))
                    model["CompressedSupportFiles"].append({ "Value": filePkl, "Key": key })

        self.model = model
        
    def __getLatticeVersion(self):
        provenanceProperties = self.mediator.provenanceProperties
        if "Lattice_Version" in provenanceProperties:
            return provenanceProperties["Lattice_Version"]
        return None
 
    @overrides(JsonGenBase)
    def getKey(self):
        return "Model"
    
    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.model
