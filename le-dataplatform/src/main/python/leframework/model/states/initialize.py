import logging

from leframework.codestyle import overrides
from leframework.model.state import State
from leframework.util.scoringutil import ScoringUtil
import pandas as pd


class Initialize(State):
    
    def __init__(self):
        State.__init__(self, "Initialize")
        self.logger = logging.getLogger(name='initialize')
    
    @overrides(State)
    def execute(self):
        mediator = self.getMediator()

        # Score and Update Data
        dataScores = ScoringUtil.score(mediator, mediator.data, self.logger)
        dataScoreSeries = pd.Series(dataScores, index=mediator.data.index)
        mediator.data[mediator.schema["reserved"]["score"]].update(dataScoreSeries)

        # Score PostTransform And Update PreTransform
        allScores = ScoringUtil.score(mediator, mediator.allDataPostTransform, self.logger)
        allScoreSeries = pd.Series(allScores, index=mediator.allDataPreTransform.index)
        mediator.allDataPreTransform[mediator.schema["reserved"]["score"]].update(allScoreSeries)
