from leframework.model import state
from leframework import codestyle as cs

class Initialize(state.State):
    
    def __init__(self):
        state.State.__init__(self, "Initialize")
    
    @cs.overrides(state.State)
    def execute(self):
        mediator = self.getMediator()
        scored = mediator.clf.predict_proba(mediator.data[:, mediator.schema["featureIndex"]])
        mediator.scored = scored
