import logging


from leframework.codestyle import overrides
from leframework.model.state import State


class Initialize(State):
    
    def __init__(self):
        State.__init__(self, "Initialize")
        self.logger = logging.getLogger(name='initialize')
    
    @overrides(State)
    def execute(self):
        mediator = self.getMediator()
        scored = self.score(mediator)
        mediator.scored = scored
        
    def score(self, mediator):
        scored = mediator.clf.predict_proba(mediator.data[:, mediator.schema["featureIndex"]])
        scored = [row[1] for row in scored]
        return scored
    
