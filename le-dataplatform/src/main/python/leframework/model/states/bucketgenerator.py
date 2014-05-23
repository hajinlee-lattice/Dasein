from leframework.model import state
from leframework.model import jsongenbase as jb
from leframework import codestyle as cs

class BucketGenerator(state.State, jb.JsonGenBase):

    def __init__(self):
        state.State.__init__(self, "BucketGenerator")
    
    @cs.overrides(state.State)
    def execute(self):
        self.buckets = []
    
    @cs.overrides(jb.JsonGenBase)
    def getKey(self):
        return "Buckets"
    
    @cs.overrides(jb.JsonGenBase)
    def getJsonProperty(self):
        return self.buckets