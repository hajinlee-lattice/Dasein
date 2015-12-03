import logging

from leframework.codestyle import overrides
from leframework.model.state import State
from leframework.util.scoringutil import ScoringUtil
import pandas as pd

import math
from numpy import percentile
'''
INPUT DATA:
revenueTest
revenueTrain
predictedRevenueTest
predictedRevenueTrain
predictedPurchaseEventTest
predictedPurchaseEventTrain

FUNCTIONS TO RUN AND REPORT RESULTS:
combineOverlapStats(predictedRevenueTrain,predictedPurchaseEventTrain,revenueTrain,.9)
combineOverlapStats(predictedRevenueTest,predictedPurchaseEventTest,revenueTest,.9)

'''

class RevenueStatistics(State):
    
    def __init__(self):
        State.__init__(self, "RevenueStatistics")
        self.logger = logging.getLogger(name='initializeRevenue')
    
    @overrides(State)
    def execute(self):
        mediator = self.getMediator()
        if mediator.revenueColumn == None:
            return
        predictedRevenue = mediator.data[mediator.schema["reserved"]["predictedrevenue"]].as_matrix()
        predictedEvent = mediator.data[mediator.schema["reserved"]["score"]].as_matrix()
        realizedRevenue = mediator.data[mediator.revenueColumn].fillna(value=0).as_matrix()
        
        mediator.revenueStatistics = self.combinedOverlapStats(predictedRevenue, predictedEvent, realizedRevenue)
        
        
    def basicStats(self, x):
        mn = float(sum(x)) / len(x)
        mn1 = float(sum([abs(y - mn) for y in x])) / len(x)
        mn2 = float(sum([(y - mn) ** 2 for y in x])) / len(x)
        return mn1, math.sqrt(mn2)

    def revenueStats(self, predicted, realized):
        diff = [predicted[i] - realized[i] for i in range(len(predicted))]
        sd1Err, sdErr = self.basicStats(diff)
        sd1R, sdR = self.basicStats(realized)
        r2 = 1.0 - (sdErr * sdErr) / (sdR * sdR)
        return [sd1Err, sdErr, r2]

    # assumes that any NA in realized are replaced by 0
    def overlapStats(self, predicted, realized, percThresh=0.9):
        indPred = [i for i, x in enumerate(predicted) if x >= percentile(predicted, percThresh * 100.0)]
        indRealized = [i for i, x in enumerate(realized) if x >= percentile(realized, percThresh * 100.0)]
        overlapPercent = len(set(indPred) & set(indRealized)) * 1.0 / len(indRealized)
        predictedPercent = 1.0 * sum(realized[i] for i in indPred) / sum(realized[i] for i in indRealized)
        return [overlapPercent, predictedPercent]

    # assumes that any NA in realized are replaced by 0
    def combinedOverlapStats(self, predictedRevenue, predictedEvent, realizedRevenue, percThresh=0.9):    
        predictedTotal = [predictedRevenue[i] * predictedEvent[i] for i in range(len(predictedRevenue))]
        rawStats = self.overlapStats(predictedRevenue, realizedRevenue)
        totalStats = self.overlapStats(predictedTotal, realizedRevenue)
        names = ('topBucketByRevenue_PercentOverlap', 'topBucketByRevenue_PercentTopRevenue',
               'topBucketByComboScore_PercentOverlap', 'topBucketByComboScore_PercentTopRevenue',
               'revenuePrediction_L1Error', 'revenuePrediction_SDError', 'revenuePrediction_R2')
        return zip(names, rawStats + totalStats + self.revenueStats(predictedRevenue, realizedRevenue))
