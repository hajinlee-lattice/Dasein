import logging
import pandas as pd

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

        #Score and Update Data
        dataScores = ScoringUtil.score(mediator, mediator.data, self.logger)
        mediator.data[mediator.schema["reserved"]["score"]].update(pd.Series(dataScores))

        # Score PostTransform And Update PreTransform
        allScores = ScoringUtil.score(self.mediator, mediator.allDataPostTransform, self.logger)
        mediator.allDataPreTransform[mediator.schema["reserved"]["score"]].update(pd.Series(allScores))

        