import logging

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State


class BucketGenerator(State, JsonGenBase):

    def __init__(self):
        State.__init__(self, "BucketGenerator")
        self.logger = logging.getLogger(name='bucketgenerator')
    
    @overrides(State)
    def execute(self):
        self.buckets = []
    
    @overrides(JsonGenBase)
    def getKey(self):
        return "Buckets"
    
    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.buckets