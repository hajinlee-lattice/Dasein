import logging


from leframework.codestyle import overrides
from leframework.model.state import State
from leframework.util.scoringutil import ScoringUtil

class Initialize(State):
    
    def __init__(self):
        State.__init__(self, "Initialize")
        self.logger = logging.getLogger(name='initialize')
    
    @overrides(State)
    def execute(self):
        mediator = self.getMediator()
        scored = ScoringUtil.score(mediator, mediator.data, self.logger)
        mediator.scored = scored
        