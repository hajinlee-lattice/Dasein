import logging
import numpy as np
from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State


class RevenueModelQualityGenerator(State, JsonGenBase):
    def __init__(self, usePeriodOffset=True):
        State.__init__(self, "ModelQualityGenerator")
        self.logger = logging.getLogger(name='modelqualitygenerator')
        self.modelquality = {}
        self.usePeriodOffset = usePeriodOffset

    @overrides(State)
    def execute(self):
        try:
            mediator = self.mediator
            schema = mediator.schema

            self.logger.info("Calculating EventScores in ModelQualityGenerator")
            event = list(mediator.data[schema["target"]])

            if self.usePeriodOffset:
                periodOffset = list(mediator.data['Offset'])
                periodOffsetMapping = self.groupedValueIndicies(periodOffset)

            mediator.modelquality = {}

            # eventRanking is "predictedEvent"
            eventRanking = self.replaceNullsWithZero(list(mediator.data[schema["reserved"]["score"]]))
            eventScores = self.calculateEventScore(event, eventRanking)
            self.modelquality["eventScores"] = {"allPeriods": eventScores}
            if self.usePeriodOffset:
                for k in periodOffsetMapping.keys():
                    ind = periodOffsetMapping[k]
                    periodOffsetName = "Period_" + str(k)
                    newEvent = [event[i] for i in ind]
                    newRanking = [eventRanking[i] for i in ind]
                    eventScores = self.calculateEventScore(newEvent, newRanking)
                    self.modelquality["eventScores"][periodOffsetName] = eventScores

            mediator.modelquality = self.modelquality
            self.logger.info("Finished calculating EventScores in ModelQualityGenerator")

            self.logger.info("Calculating ValueScores in ModelQualityGenerator")

            # Check if EV model
            if mediator.revenueColumn is not None:

                valueSpent = self.replaceNullsWithZero(list(mediator.data[mediator.revenueColumn]))
                valueRanking = self.replaceNullsWithZero(
                        list(mediator.data[mediator.schema["reserved"]["predictedrevenue"]]))

                expectedValueRanking = [valueRanking[i] * eventRanking[i] for i in range(len(valueRanking))]
                expectedValueScores = self.calculateValueScore(valueSpent, expectedValueRanking, eventRanking,
                                                               valueRanking, expectedValueRanking)
                self.modelquality["expectedValueScores"] = {"allPeriods": expectedValueScores}
                self.logger.info("Finished Calculating ExpectedValueScores in ModelQualityGenerator")

                propensityValueScores = self.calculateValueScore(valueSpent, eventRanking, eventRanking, valueRanking,
                                                                 expectedValueRanking)
                self.modelquality["propensityValueScores"] = {"allPeriods": propensityValueScores}
                self.logger.info("Finished Calculating PropensityValueScores in ModelQualityGenerator")

                predictedValueValueScores = self.calculateValueScore(valueSpent, valueRanking, eventRanking,
                                                                     valueRanking, expectedValueRanking)
                self.modelquality["predictedValueValueScores"] = {"allPeriods": predictedValueValueScores}
                self.logger.info("Finished Calculating PredictedValueValueScores in ModelQualityGenerator")

                precisionRecallValueMatrix = self.precisionRecallMatrix(valueRanking, valueSpent)
                precisionRecallEVMatrix = self.precisionRecallMatrix(expectedValueRanking, valueSpent)
                simplifiedPrecisionRecallEVMatrix = self.simplifyPrecisionRecallResults(precisionRecallEVMatrix)
                self.modelquality["precisionRecallValueMatrix"] = {"allPeriods": precisionRecallValueMatrix}
                self.modelquality["precisionRecallEVMatrix"] = {"allPeriods": precisionRecallEVMatrix}
                self.modelquality["simplifiedPrecisionRecallEVMatrix"] = {
                    "allPeriods": simplifiedPrecisionRecallEVMatrix}
                self.logger.info("Finished Calculating Precision Recall Matricies in ModelQualityGenerator")

                if self.usePeriodOffset:
                    for k in periodOffsetMapping.keys():
                        ind = periodOffsetMapping[k]
                        periodOffsetName = "Period_" + str(k)
                        valueSpent_Period = [valueSpent[i] for i in ind]
                        expectedValueRanking_Period = [expectedValueRanking[i] for i in ind]
                        valueRanking_Period = [valueRanking[i] for i in ind]
                        eventRanking_Period = [eventRanking[i] for i in ind]

                        expectedValueScores = self.calculateValueScore(valueSpent_Period, expectedValueRanking_Period,
                                                                       eventRanking_Period, valueRanking_Period,
                                                                       expectedValueRanking_Period)
                        propensityValueScores = self.calculateValueScore(valueSpent_Period, eventRanking_Period,
                                                                         eventRanking_Period, valueRanking_Period,
                                                                         expectedValueRanking_Period)

                        predictedValueValueScores = self.calculateValueScore(valueSpent_Period, valueRanking_Period,
                                                                             eventRanking_Period, valueRanking_Period,
                                                                             expectedValueRanking_Period)
                        self.modelquality["expectedValueScores"][periodOffsetName] = expectedValueScores
                        self.modelquality["propensityValueScores"][periodOffsetName] = propensityValueScores
                        self.modelquality["predictedValueValueScores"][periodOffsetName] = predictedValueValueScores

            mediator.modelquality = self.modelquality
        except Exception as e:
            self.logger.error("Caught error in ModelQualityGenerator")
            self.logger.error(e)
            mediator.modelquality = None

    def sortedInd(self, x, toReverse=True):
        return sorted(range(len(x)), key=lambda i: x[i], reverse=toReverse)

    def replaceNullsWithZero(self, x):
        def fix(y):
            if np.isnan(y):
                return 0.0
            return y

        return [fix(z) for z in x]

    def cleanVal(self, xList):
        sx = set()
        for i in range(len(xList)):
            vx = set([i for i, x in enumerate(xList[i]) if np.isnan(x)])
            sx = sx | vx
        px = [i for i in range(len(xList[0])) if i not in sx]
        return [[xList[i][j] for j in px]]

    def groupedValueIndicies(self, x):
        return {k: [y[0] for y in enumerate(x) if y[1] == k] for k in set(x)}

    def calculateValueScore(self, valSpent, ranking, eventRanking=None, valueRanking=None, expectedValueRanking=None):
        length = len(valSpent)
        bucketNumber = int(min(100.0,float(length)/10))
        if bucketNumber<4: return {}
        if length != len(ranking): return None
        # sort by rank and by value spent to see how well it works
        indR = self.sortedInd(ranking)
        indV = self.sortedInd(valSpent)
        totalSpent = float(sum(valSpent))
        if totalSpent == 0:
            totalSpent = 1
        # cumulative sum of percentage of total value spent ordered by ranking
        valTotalSpent = np.cumsum([valSpent[i] / totalSpent for i in indR])
        valTotalRanked = np.cumsum([valSpent[i] / totalSpent for i in indV])
        aucSpent = np.mean(valTotalSpent)
        aucRanked = np.mean(valTotalRanked)

        def evalRev(perc):
            ix = range(0, int(perc * length))
            indRankInBin = [indR[i] for i in ix]
            indValueInBin = [indV[i] for i in ix]
            # Make sure all values are non-zero
            if sum(indValueInBin) == 0:
                return (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
            lenOverlap = len(set(indRankInBin) & set(indValueInBin))
            totalRevenue = float(sum([valSpent[i] for i in indRankInBin]))
            totalConversions = float(sum([valSpent[i] != 0 for i in indRankInBin]))
            MaxTotalRevenue = float(sum([valSpent[i] for i in indValueInBin]))
            sumRanking = float(sum([ranking[i] for i in indRankInBin]))
            popSize = len(indRankInBin)

            if ranking is None:
                sumRanking = 0.0
            else:
                sumEventPred = float(sum([ranking[i] for i in indRankInBin]))

            if eventRanking is None:
                sumEventPred = 0.0
            else:
                sumEventPred = float(sum([eventRanking[i] for i in indRankInBin]))

            if valueRanking is None:
                sumValuePred = 0.0
            else:
                sumValuePred = float(sum([valueRanking[i] for i in indRankInBin]))

            if expectedValueRanking is None:
                sumEVPred = 0.0
            else:
                sumEVPred = float(sum([expectedValueRanking[i] for i in indRankInBin]))

            if MaxTotalRevenue > 0 and len(ix) > 0:
                returnTuple = (float(lenOverlap) / len(ix),
                               popSize,
                               totalRevenue / popSize,
                               totalConversions / popSize,
                               totalRevenue / MaxTotalRevenue,
                               sumRanking / len(ix),
                               sumEventPred / len(ix),
                               sumValuePred / len(ix),
                               sumEVPred / len(ix))
            else:
                returnTuple = (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
            return returnTuple

        bucketIncr = 1.0 / bucketNumber
        percMaxCount, popSize, meanRev, convRate, percTotalRev, meanRanking, meanPredictedEvent, meanPredictedRevenue, meanEV = zip(
                *[evalRev(bucketIncr * i) for i in range(1, bucketNumber+1)])

        results = {}
        results["percMaxCount"] = percMaxCount
        results["popSize"] = popSize
        results["meanRev"] = meanRev
        results["convRate"] = convRate
        results["percTotalRev"] = percTotalRev
        results["meanRanking"] = meanRanking
        results["meanPredictedEvent"] = meanPredictedEvent
        results["meanPredictedRevenue"] = meanPredictedRevenue
        results["meanEV"] = meanEV
        results["aucSpent"] = aucSpent
        results["aucRanked"] = aucRanked
        results["aucQuality"] = (aucRanked / aucSpent) if aucSpent != 0 else 0

        return results

    def precisionRecallMatrix(self, ranking, valSpent):
        length = len(valSpent)
        bucketNumber = int(min(100.0,float(length)/10))
        if bucketNumber<4: return {}
        if length != len(ranking): return None
        indR = self.sortedInd(ranking, False)
        indV = self.sortedInd(valSpent, False)

        def binDetails(indicies):
            rankingIndexLow, valueIndexLow = indicies
            rank = ranking[indR[rankingIndexLow]]
            value = valSpent[indV[valueIndexLow]]
            valInd = [indV[i] for i in range(valueIndexLow, length)]
            rankInd = [indR[i] for i in range(rankingIndexLow, length)]
            cnt = len(set(valInd) & set(rankInd))
            if len(rankInd) == 0:
                return None
            return rank, value, cnt / 1.0 / length, cnt / 1.0 / len(valInd), cnt / 1.0 / len(rankInd)

        bucketIncr = 1.0 / bucketNumber
        ix = [int(i * bucketIncr * length) for i in range(0, bucketNumber)]
        indexList = [(i, j) for i in ix for j in ix]
        return [binDetails(x) for x in indexList]

    def calculateEventScore(self, event, ranking):
        length = len(event)
        numBuckets = int(min(100.0,float(length)/10))
        if numBuckets < 4: return {}
        if length != len(ranking):
            return None
        indR = self.sortedInd(ranking)
        totalEvents = float(sum(event))
        if totalEvents == 0:
            totalEvents = 1
        percentEvents = np.cumsum([event[i] / totalEvents for i in indR])
        auc = np.mean(percentEvents)
        liftCurve = [percentEvents[i] * len(percentEvents) / float(i + 1) for i in range(len(percentEvents))]
        bucketIncr = 1.0 / numBuckets
        liftBuckets = [int(i * bucketIncr * len(liftCurve)) for i in range(1, numBuckets + 1)]
        liftBuckets[len(liftBuckets) - 1] = len(liftCurve) - 1
        outPutLiftCurve = [((b + 1.0) / float(len(liftCurve)), liftCurve[b]) for b in liftBuckets]

        results = {}
        results["auc"] = auc
        results['conversionRate'] = totalEvents / len(event)
        results["outputLiftCurve"] = outPutLiftCurve
        return results

    def simplifyPrecisionRecallResults(self, PRMatrix):
        numBuckets = int(np.sqrt(len(PRMatrix)))
        # take out percentile thresholds
        predictions = [x[0] for x in PRMatrix[0::numBuckets]]
        revenues = [x[1] for x in PRMatrix[:numBuckets]]
        # fold the percentage distribution into 100x100 matrix
        percentages = np.reshape([x[2] for x in PRMatrix], (numBuckets, numBuckets))
        recalls = np.reshape([x[3] for x in PRMatrix], (numBuckets, numBuckets))
        precisions = np.reshape([x[4] for x in PRMatrix], (numBuckets, numBuckets))

        def roundTo(x, sigDigits=1):
            """
            round a value to certain significant digits
            """
            if x == 0:
                return 0
            else:
                return round(x, sigDigits - 1 - int(np.floor(np.log10(abs(x)))))

        # event needs to have at least 2% presence
        # then find eligible thresholds, which has 1 significant digit
        pctToExcl = int(0.02 * numBuckets)
        eventThresholds = sorted(set([roundTo(x) for x in revenues[pctToExcl:numBuckets-pctToExcl]]))

        def thresholdDetails(threshold, recall):
            actualThreshold = min(revenues, key=lambda x: abs(x - threshold))
            valInd = revenues.index(actualThreshold)
            percentage = percentages[0, valInd]
            actualRecall = min(recalls[:, valInd], key=lambda x: abs(x - recall))
            rankInd = recalls[:, valInd].tolist().index(actualRecall)
            precision = precisions[rankInd, valInd]
            cutoff = predictions[rankInd]
            return threshold, actualThreshold, percentage, recall, actualRecall, precision, cutoff

        recallTargets = [0.01 * x for x in range(20, 85, 5)]
        combinations = [(e, r) for e in eventThresholds for r in recallTargets]
        return [thresholdDetails(*c) for c in combinations]

    @overrides(JsonGenBase)
    def getKey(self):
        return "ModelQuality"

    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.modelquality