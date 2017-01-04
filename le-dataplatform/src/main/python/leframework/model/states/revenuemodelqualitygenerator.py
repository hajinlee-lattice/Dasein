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
            event = list(mediator.data[schema["target"]])
            
#             periodID=list(mediator.data['Period_ID'])
#             periodMapping=self.groupedValueIndicies(periodID)

            mediator.modelquality = {}

            # eventRanking is "predictedEvent"
            eventRanking = list(mediator.data[schema["reserved"]["score"]])
            eventScores = self.calculateEventScore(event, eventRanking)
            self.modelquality["eventScores"] = {"allPeriods":eventScores}


#             for k in periodMapping.keys():
#                 ind=periodMapping[k]
#                 periodName="Period_"+str(k)
#                 newEvent=[event[i] for i in ind]
#                 newRanking=[eventRanking[i] for i in ind]
#                 #eventScores=self.calculateEventScore(event.iloc[ind], eventRanking.iloc[ind])
#                 eventScores=self.calculateEventScore(newEvent,newRanking)
#                 self.modelquality["eventScores"][periodName]=eventScores


            mediator.modelquality = self.modelquality
            self.logger.info("Finished calculating EventScores in ModelQualityGenerator")

            self.logger.info("Calculating ValueScores in ModelQualityGenerator")
            
            # Check if EV model
            if mediator.revenueColumn != None:

                valueSpent = list(mediator.data[mediator.revenueColumn] )
                valueSpent = [0 if np.isnan(x) else x for x in valueSpent]
                valueRanking = list(mediator.data[mediator.schema["reserved"]["predictedrevenue"]])
                
                expectedValueRanking=[valueRanking[i] * eventRanking[i] for i in range(len(valueRanking))]
                expectedValueScores = self.calculateValueScore(valueSpent, expectedValueRanking,eventRanking,valueRanking,expectedValueRanking)
                self.modelquality["expectedValueScores"] =  {"allPeriods":expectedValueScores}
                self.logger.info("Finished Calculating ExpectedValueScores in ModelQualityGenerator")
                
                propensityValueScores=self.calculateValueScore(valueSpent, eventRanking,eventRanking,valueRanking,expectedValueRanking)
                self.modelquality["propensityValueScores"] =  {"allPeriods":propensityValueScores}
                self.logger.info("Finished Calculating PropensityValueScores in ModelQualityGenerator")
                
                predictedValueValueScores=self.calculateValueScore(valueSpent, valueRanking,eventRanking,valueRanking,expectedValueRanking)
                self.modelquality["predictedValueValueScores"] =  {"allPeriods":predictedValueValueScores}
                self.logger.info("Finished Calculating PredictedValueValueScores in ModelQualityGenerator")

#                 for k in periodMapping.keys():
#                     ind=periodMapping[k]
#                     periodName="Period_"+str(k)
#                     valueSpent_Period=[valueSpent[i] for i in ind]
#                     expectedValueRanking_Period=[expectedValueRanking[i] for i in ind]
#                     valueRanking_Period=[valueRanking[i] for i in ind]
#                     eventRanking_Period=[eventRanking[i] for i in ind]
#                     expectedValueScores = self.calculateValueScore(valueSpent_Period, expectedValueRanking_Period,eventRanking_Period,valueRanking_Period,expectedValueRanking_Period)
#                     propensityValueScores=self.calculateValueScore(valueSpent_Period, eventRanking_Period,eventRanking_Period,valueRanking_Period,expectedValueRanking_Period)
#                     predictedValueValueScores=self.calculateValueScore(valueSpent_Period, valueRanking_Period,eventRanking_Period,valueRanking_Period,expectedValueRanking_Period)
#                     self.modelquality["expectedValueScores"][periodName] =expectedValueScores
#                     self.modelquality["propensityValueScores"][periodName]=propensityValueScores
#                     self.modelquality["predictedValueValueScores"][periodName] = predictedValueValueScores


            mediator.modelquality = self.modelquality
        except Exception as e:
            self.logger.error("Caught error in ModelQualityGenerator")
            self.logger.error(e)
            mediator.modelquality = None

    def sortedInd(self, x, toReverse=True):
        return sorted(range(len(x)), key=lambda i : x[i], reverse=toReverse)

    def groupedValueIndicies(self,x):
        return {k:[y[0] for y in enumerate(x) if y[1]==k] for k in set(x)}

    def calculateValueScore(self, valSpent, ranking,eventRanking=None,valueRanking=None,expectedValueRanking=None):
        length = len(valSpent)
        if length != len(ranking): return None
        # sort by rank and by value spent to see how well it works
        indR = self.sortedInd(ranking)
        indV = self.sortedInd(valSpent)
        totalSpent = float(sum(valSpent))
        if totalSpent == 0:
            return (0.0 , 0.0, 0.0, 0.0, 0.0, 0.0,0.0,0.0,0.0)
        # cumulative sum of percentage of total value spent ordered by ranking
        valTotal = np.cumsum([valSpent[i] / totalSpent for i in indR])
        auc = np.mean(valTotal)
        def evalRev(perc):
            ix = range(0, int(perc * length))
            indRankInBin = [indR[i] for i in ix]
            indValueInBin = [indV[i] for i in ix]
            # Make sure all values are non-zero
            if sum(indValueInBin) == 0:
                return (0.0 , 0.0, 0.0, 0.0, 0.0, 0.0,0.0,0.0,0.0)
            lenOverlap = len(set(indRankInBin) & set(indValueInBin))
            totalRevenue = float(sum([valSpent[i] for i in indRankInBin]))
            totalConversions = float(sum([valSpent[i]!=0 for i in indRankInBin]))
            MaxTotalRevenue = float(sum([valSpent[i] for i in indValueInBin]))
            sumRanking=float(sum([ranking[i] for i in indRankInBin]))
            popSize=len(indRankInBin)

            if ranking is None:
                sumRanking=0.0
            else:
                sumEventPred=float(sum([ranking[i] for i in indRankInBin]))

                
            if eventRanking is None:
                sumEventPred=0.0
            else:
                sumEventPred=float(sum([eventRanking[i] for i in indRankInBin]))
                
            if valueRanking is None:
                sumValuePred=0.0
            else:
                sumValuePred=float(sum([valueRanking[i] for i in indRankInBin]))
                
            if expectedValueRanking is None:
                sumEVPred=0.0
            else:
                sumEVPred=float(sum([expectedValueRanking[i] for i in indRankInBin]))

            if MaxTotalRevenue > 0 and len(ix) > 0 :
                returnTuple=(float(lenOverlap)/len(ix),
                             popSize,
                             totalRevenue/popSize,
                             totalConversions/popSize,
                             totalRevenue/MaxTotalRevenue,
                             sumRanking/len(ix),
                             sumEventPred/len(ix),
                             sumValuePred/len(ix),
                             sumEVPred/len(ix))
            else:
                returnTuple=(0.0 , 0.0, 0.0, 0.0, 0.0, 0.0,0.0,0.0,0.0)
            return returnTuple
        percMaxCount, popSize, meanRev, convRate, percTotalRev,meanRanking,meanPredictedEvent,meanPredictedRevenue,meanEV = zip(*[evalRev(.01 * i) for i in range(1, 101)])

        results = {}
        results["percMaxCount"] = percMaxCount
        results["popSize"] = popSize
        results["meanRev"] = meanRev
        results["convRate"]=convRate
        results["percTotalRev"] = percTotalRev
        results["meanRanking"]=meanRanking
        results["meanPredictedEvent"]=meanPredictedEvent
        results["meanPredictedRevenue"]=meanPredictedRevenue
        results["meanEV"]=meanEV
    
        
        return results

    def calculateEventScore(self, event, ranking, liftBuckets=0.01):
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
        results['conversionRate'] = totalEvents/len(event)
        results["outputLiftCurve"] = outPutLiftCurve

        return results

    @overrides(JsonGenBase)
    def getKey(self):
        return "ModelQuality"

    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.modelquality
