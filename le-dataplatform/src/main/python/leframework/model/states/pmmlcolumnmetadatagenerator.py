from collections import OrderedDict
import logging

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State


class PmmlColumnMetadataGenerator(State, JsonGenBase):

    def __init__(self):
        State.__init__(self, "PmmlColumnMetadataGenerator")
        self.logger = logging.getLogger(name='pmmlcolumnmetadatagenerator')
    
    @overrides(State)
    def execute(self):
        self.inputColumnMetadata = []
        mediator = self.mediator
        
        metadata = mediator.metadata[1]
        fields = mediator.schema["original_features"]
        
        fieldsWithTypes = mediator.schema["fields"]
        
        for field in fields:
            f = OrderedDict()
            record = None
            f["Description"] = field
            
            f["Name"] = field
            f["Purpose"] = 3
            if field in mediator.schema["targets"]:
                f["Purpose"] = 4
            
            f["ValueType"] = self.__getValueType(fieldsWithTypes[field])
            
            self.inputColumnMetadata.append(f)
    
    def __getValueType(self, dataType):
        if dataType == "string" or dataType == "bytes":
            return 1
        return 0
    
    @overrides(JsonGenBase)
    def getKey(self):
        return "InputColumnMetadata"
    
    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.inputColumnMetadata