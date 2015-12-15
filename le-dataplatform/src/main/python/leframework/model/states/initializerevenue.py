import logging

from leframework.codestyle import overrides
from leframework.model.state import State
from leframework.util.scoringutil import ScoringUtil
import pandas as pd
import math


class InitializeRevenue(State):
    
    def __init__(self):
        State.__init__(self, "InitializeRevenue")
        self.logger = logging.getLogger(name='InitializeRevenue')
    
    @overrides(State)
    def execute(self):
        mediator = self.getMediator()
        
        if mediator.revenueColumn == None:
            return
        # Get revenue and Update Data
#         dataRevenues = ScoringUtil.(mediator, mediator.data, self.logger)
        dataRevenues = mediator.clf.predict_regression(mediator.data[mediator.schema["features"]])
        dataRevenueSeries = pd.Series(dataRevenues, index=mediator.data.index)
        mediator.data[mediator.schema["reserved"]["predictedrevenue"]].update(dataRevenueSeries)
        mediator.data[mediator.schema["reserved"]["predictedrevenue"]] = mediator.data[mediator.schema["reserved"]["predictedrevenue"]].apply(lambda x : math.exp(x) - 1.0)
        
