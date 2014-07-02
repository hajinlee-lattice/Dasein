import logging

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State

class PercentileBucketGenerator(State, JsonGenBase):

    def __init__(self):
        State.__init__(self, "PercentileBucketGenerator")
        self.logger = logging.getLogger(name = 'percentilebucketgenerator')
    
    @overrides(State)
    def execute(self): pass

    @overrides(JsonGenBase)
    def getKey(self):
        return "PercentileBuckets"
