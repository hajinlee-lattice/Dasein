from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State


class ColumnMetadataGenerator(State, JsonGenBase):

    def __init__(self):
        State.__init__(self, "ColumnMetadataGenerator")
    
    @overrides(State)
    def execute(self):
        self.inputColumnMetadata = []
    
    @overrides(JsonGenBase)
    def getKey(self):
        return "InputColumnMetadata"
    
    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.inputColumnMetadata