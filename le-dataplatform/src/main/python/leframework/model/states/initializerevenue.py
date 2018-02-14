import logging
import pandas as pd
from leframework.codestyle import overrides
from leframework.model.state import State


class InitializeRevenue(State):
    
    def __init__(self):
        State.__init__(self, "InitializeRevenue")
        self.logger = logging.getLogger(name='InitializeRevenue')
    
    @overrides(State)
    def execute(self):
        mediator = self.getMediator()
        
        if mediator.revenueColumn is None:
            return
        # Get revenue and Update Data
#         dataRevenues = ScoringUtil.(mediator, mediator.data, self.logger)
        dataRevenues = mediator.clf.predict_regression(mediator.data[mediator.schema["features"]])
        if dataRevenues is None or dataRevenues.all(None):
            return
        dataRevenueSeries = pd.Series(dataRevenues, index=mediator.data.index)
        mediator.data[mediator.schema["reserved"]["predictedrevenue"]].update(dataRevenueSeries)
        mediator.data[mediator.schema["reserved"]["expectedrevenue"]] = mediator.data[mediator.schema["reserved"]["predictedrevenue"]] * mediator.data[mediator.schema["reserved"]["score"]] 
