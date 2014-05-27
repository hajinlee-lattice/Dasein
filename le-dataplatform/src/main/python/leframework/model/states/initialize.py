from leframework.codestyle import overrides
from leframework.model.state import State

class Initialize(State):
    
    def __init__(self):
        State.__init__(self, "Initialize")
    
    @overrides(State)
    def execute(self):
        mediator = self.getMediator()
        scored = mediator.clf.predict_proba(mediator.data[:, mediator.schema["featureIndex"]])
        mediator.scored = scored
