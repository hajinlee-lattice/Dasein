import logging

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State
import numpy as np


class RevenueModelQualityGenerator(State, JsonGenBase):

    def __init__(self):
        State.__init__(self, "ModelQualityGenerator")
        self.logger = logging.getLogger(name='modelqualitygenerator')
        self.modelquality = {}

    @overrides(State)
    def execute(self):
        try:
            mediator = self.mediator
            schema = mediator.schema

            self.logger.info("Calculating EventScores in ModelQualityGenerator")
            event = mediator.data[schema["target"]]
            mediator.modelquality = {}

            # eventRanking is "predictedEvent"
            eventRanking = mediator.data[schema["reserved"]["score"]]
            eventScores = self.calculateEventScore(event, eventRanking)
            self.modelquality["eventScores"] = eventScores
            mediator.modelquality = self.modelquality
            self.logger.info("Finished calculating EventScores in ModelQualityGenerator")

            self.logger.info("Calculating ValueScores in ModelQualityGenerator")
            # Check if EV model
            if mediator.revenueColumn != None:
                valueSpent = mediator.data[mediator.schema["reserved"]["predictedrevenue"]]
                valueRanking = eventRanking
                valueScores = self.calculateValueScore(valueSpent, valueRanking)
                self.modelquality["valueScores"] = valueScores
                self.logger.info("Finished Calculating ValueScores in ModelQualityGenerator")

            mediator.modelquality = self.modelquality
        except Exception as e:
            self.logger.error("Caught error in ModelQualityGenerator")
            self.logger.error(e)
            mediator.modelquality = None

    def sortedInd(self, x, toReverse=True):
        return sorted(range(len(x)), key=lambda i : x[i], reverse=toReverse)

    def calculateValueScore(self, valSpent, ranking):
        length = len(valSpent)
        if length != len(ranking): return None
        # sort by rank and by value spent to see how well it works
        indR = self.sortedInd(ranking)
        indV = self.sortedInd(valSpent)
        totalSpent = float(sum(valSpent))
        # cumulative sum of percentage of total value spent ordered by ranking
        valTotal = np.cumsum([valSpent[i] / totalSpent for i in indR])
        auc = np.mean(valTotal)
        def evalRev(perc):
            ix = range(0, int(perc * length))
            indRankInBin = [indR[i] for i in ix]
            indValueInBin = [indV[i] for i in ix]
            # Make sure all values are non-zero
            if sum(indValueInBin) == 0:
                return 0.0, 0.0
            lenOverlap = len(set(indRankInBin) & set(indValueInBin))
            totalRevenue = float(sum([valSpent[i] for i in indRankInBin]))
            MaxTotalRevenue = float(sum([valSpent[i] for i in indValueInBin]))
            if MaxTotalRevenue > 0 and len(ix) > 0 :
                return float(lenOverlap) / len(ix), totalRevenue / MaxTotalRevenue
            else:
                return 0.0 , 0.0
        percMaxCount, percTotalRev = zip(*[evalRev(.01 * i) for i in range(0, 100)])

        results = {}
        results["percMaxCount"] = percMaxCount
        results["percTotalRev"] = percTotalRev

        return results

    def calculateEventScore(self, event, ranking, liftBuckets=0.1):
        length = len(event)
        if length != len(ranking):
            return None
        indR = self.sortedInd(ranking)
        totalEvents = float(sum(event))
        percentEvents = np.cumsum([event[i] / totalEvents for i in indR])
        auc = np.mean(percentEvents)
        liftCurve = [percentEvents[i] * len(percentEvents) / float(i + 1) for i in range(len(percentEvents))]
        numBuckets = int(1.0 / liftBuckets)
        liftBuckets = [int(i * liftBuckets * len(liftCurve)) for i in range(1, numBuckets + 1)]
        liftBuckets[len(liftBuckets) - 1] = len(liftCurve) - 1
        outPutLiftCurve = [((b + 1.0) / float(len(liftCurve)), liftCurve[b]) for b in liftBuckets]

        results = {}
        results["auc"] = auc
        results["outputLiftCurve"] = outPutLiftCurve

        return results

    @overrides(JsonGenBase)
    def getKey(self):
        return "ModelQuality"

    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.modelquality
