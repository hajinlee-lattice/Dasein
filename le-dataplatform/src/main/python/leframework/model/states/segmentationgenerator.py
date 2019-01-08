from collections import OrderedDict
import logging

from leframework.codestyle import overrides
from leframework.model.state import State
from leframework.util.scoringutil import ScoringUtil
from leframework.model.states.fitfunctionutil import FitFunctionUtil
from leframework.util.pdversionutil import pd_before_17
import numpy as np
import pandas as pd
import math
import json

class SegmentationGenerator(State):

    def __init__(self):
        State.__init__(self, "SegmentationGenerator")
        self.logger = logging.getLogger(name='segmentationgenerator')

    @overrides(State)
    def execute(self):
        mediator = self.mediator
        schema = mediator.schema

        orderedScore = self.mediator.data[[schema["reserved"]["score"], schema["target"]]]
        orderedScore = ScoringUtil.sortWithRandom(orderedScore, 0)
        numLeads = orderedScore.shape[0]

        # Break Into 1% Granularity
        numSegments = 100
        numBlocks = numSegments
        blockSize = numLeads / numBlocks
        remainder = numLeads - (numBlocks * blockSize)
        segments = [0] * numBlocks

        offset = [0]
        def addSegment(i):
            lower = offset[0]; upper = min(lower + blockSize, numLeads)
            segmentScores = orderedScore[lower:upper]
            segment = OrderedDict()
            segment["Score"] = (100 - i)
            segment["Count"] = segmentScores.shape[0]
            segment["Converted"] = int(segmentScores[schema["target"]].sum())
            segments[i] = segment
            offset[0] = upper

        if remainder > 0:
            blockSize += 1
            for i in xrange(0, remainder):
                addSegment(i)
            blockSize -= 1

        for i in xrange(remainder, numBlocks):
            addSegment(i)

        # Construct Result
        self.result = []
        allSegments = OrderedDict()
        allSegments["LeadSource"] = "All"
        allSegments["Segments"] = segments
        self.result.append(allSegments)

        # Add Result to Mediator
        self.mediator.segmentations = self.result


class RevenueSegmentationGenerator(State):

    def __init__(self):
        State.__init__(self, "RevenueSegmentationGenerator")
        self.logger = logging.getLogger(name='revenuesegmentationgenerator')

    @overrides(State)
    def execute(self):
        mediator = self.mediator
        if mediator.revenueColumn is None:
            return

        try:
            result = self.calculateRevenueSegments(mediator)
            self.mediator.revenuesegmentations = result
        except Exception as exp:
            self.logger.exception("Caught Exception while generating revenue segmentations: " + str(exp))
            self.mediator.revenuesegmentations = None

        self.logger.info("revenue segmentations = " + str(self.mediator.revenuesegmentations))

    def padData(self, data):
        size = data.shape[0]
        num_of_extra_copies = int(math.ceil(100. / size)) - 1
        copy = data.copy()
        for i in range(num_of_extra_copies):
            data = pd.concat([data, copy], ignore_index=True)
        return data

    def calculateRevenueSegments(self, mediator):
        schema = mediator.schema
        revColName = schema["reserved"]["predictedrevenue"]
        revenueScore = self.get_revenue_score_data_frame()
        positiveEvents = revenueScore[schema['target']] > 0
        positiveEventRevenueScore = revenueScore[positiveEvents].copy()
        if positiveEventRevenueScore.shape[0] < 100:
            positiveEventRevenueScore = self.padData(positiveEventRevenueScore)
        positiveEventRevenueScore[revColName] = positiveEventRevenueScore[revColName] + \
                                                np.random.uniform(-1, 1, size=positiveEventRevenueScore.shape[0]) * 1e-4
        if pd_before_17():
            orderedScore = positiveEventRevenueScore.sort(revColName, ascending=False).reset_index(drop=True)
        else:
            orderedScore = positiveEventRevenueScore.sort_values(by=revColName, ascending=False).reset_index(drop=True)

        numLeads = orderedScore.shape[0]
        apxBlockSize = numLeads / 100.0
        orderedScore['Score'] = orderedScore.index.map(lambda x: min(int(x / apxBlockSize) + 1, 100))
        revRateChartDf = orderedScore.groupby('Score')[schema["reserved"]["predictedrevenue"]].agg(
            ['min', 'max', 'mean', 'sum', 'count'])
        revRateChartDf.columns = ['Min', 'Max', 'Mean', 'Sum', 'Count']
        new_segments = []
        for index in range(revRateChartDf.shape[0]):
            segment = OrderedDict()
            segment["Score"] = (100 - index)
            segment["Count"] = revRateChartDf.iloc[index]['Count']
            segment["Converted"] = revRateChartDf.iloc[index]['Sum']
            segment["Sum"] = revRateChartDf.iloc[index]['Sum']
            segment["Mean"] = revRateChartDf.iloc[index]['Mean']
            segment["Max"] = revRateChartDf.iloc[index]['Max']
            segment["Min"] = revRateChartDf.iloc[index]['Min']
            new_segments.append(segment)
        # Construct Result
        result = []
        allSegments = OrderedDict()
        allSegments["LeadSource"] = "All"
        allSegments["Segments"] = new_segments
        result.append(allSegments)
        return result

    def get_revenue_score_data_frame(self):
        mediator = self.mediator
        schema = mediator.schema
        return self.mediator.data[[schema["reserved"]["predictedrevenue"], schema["target"]]]


class EVSegmentationGenerator(State):

    def __init__(self):
        State.__init__(self, "EVSegmentationGenerator")
        self.logger = logging.getLogger(name='evsegmentationgenerator')

    @overrides(State)
    def execute(self):
        mediator = self.getMediator()
        if mediator.revenueColumn is None:
            return

        try:
            result = self.calculateEVSegments(mediator)
            self.mediator.evsegmentations = result
        except Exception as exp:
            self.logger.exception("Caught Exception while generating EV segmentations: " + str(exp))
            self.mediator.evsegmentations = None

        self.logger.info("EV segmentations = " + str(self.mediator.evsegmentations))

    def calculateEVSegments(self, mediator):
        schema = mediator.schema
        revenueScoreColumn = schema["reserved"]["predictedrevenue"]
        probScoreColumn = schema["reserved"]["score"]
        revenueColumn = mediator.revenueColumn
        testScoreDf = self.mediator.data[[revenueScoreColumn, probScoreColumn, revenueColumn]]
        scoreDerivation = mediator.score_derivation
        revenueScoreDerivation = mediator.revenue_score_derivation
        bucketsProb, upperBoundsProb = self.getPercScoreBounds(scoreDerivation)
        bucketsRev, upperBoundsRev = self.getPercScoreBounds(revenueScoreDerivation)
        alphaProb, betaProb, gammaProb, maxProbRate = self.getProbFitFunctionParameters()
        alphaRev, betaRev, gammaRev, maxRevRate = self.getRevenueFitFunctionParameters()
        testScoreDf['mappedProbPerc'] = bucketsProb[np.digitize(testScoreDf[probScoreColumn], upperBoundsProb)]
        testScoreDf['mappedRevPerc'] = bucketsRev[np.digitize(testScoreDf[revenueScoreColumn], upperBoundsRev)]
        testScoreDf['mappedProb'] = testScoreDf['mappedProbPerc'].apply(lambda x:
                                                                        FitFunctionUtil.getMappedRate(x, betaProb,
                                                                                                      alphaProb,
                                                                                                      gammaProb,
                                                                                                      maxProbRate))
        testScoreDf['mappedRev'] = testScoreDf['mappedRevPerc'].apply(lambda x:
                                                                      FitFunctionUtil.getMappedRate(x, betaRev,
                                                                                                    alphaRev,
                                                                                                    gammaRev,
                                                                                                    maxRevRate))
        testScoreDf['ev'] = testScoreDf['mappedProb'] * testScoreDf['mappedRev']
        orderedScore = ScoringUtil.sortWithRandom(testScoreDf, testScoreDf.columns.get_loc('ev')).copy().reset_index(drop=True)
        numLeads = orderedScore.shape[0]
        apxBlockSize = numLeads / 100.0
        orderedScore['Score'] = orderedScore.index.map(lambda x: min(int(x / apxBlockSize) + 1, 100))

        revRateChartDf = orderedScore.groupby('Score')['ev'].agg(
            ['min', 'max', 'mean', 'sum', 'count'])

        revRateChartDf.columns = ['Min', 'Max', 'Mean', 'Sum', 'Count']
        new_segments = []
        lowestScore = 100
        for index in range(revRateChartDf.shape[0]):
            lowestScore = (100 - index)
            segment = OrderedDict()
            segment["Score"] = lowestScore
            segment["Count"] = revRateChartDf.iloc[index]['Count']
            segment["Converted"] = revRateChartDf.iloc[index]['Sum']
            segment["Sum"] = revRateChartDf.iloc[index]['Sum']
            segment["Mean"] = revRateChartDf.iloc[index]['Mean']
            segment["Max"] = revRateChartDf.iloc[index]['Max']
            segment["Min"] = revRateChartDf.iloc[index]['Min']
            new_segments.append(segment)

        if lowestScore > 1:
            lowestScore = lowestScore - 1
            for index in range(lowestScore):
                segment = OrderedDict()
                segment["Score"] = (lowestScore - index)
                segment["Count"] = 0
                segment["Converted"] = 0
                segment["Sum"] = 0
                segment["Mean"] = 0
                segment["Max"] = 0
                segment["Min"] = 0
                new_segments.append(segment)

        # Construct Result
        result = []
        allSegments = OrderedDict()
        allSegments["LeadSource"] = "All"
        allSegments["Segments"] = new_segments
        result.append(allSegments)
        return result

    def getPercScoreBounds(self, scoreDerivation):
        percentilesDf = pd.DataFrame.from_records(scoreDerivation['percentiles'])
        percentilesDf = percentilesDf.groupby(by='name').agg('max')

        percentilesDf.sort_index(inplace=True)
        buckets = np.asarray(percentilesDf.index)
        buckets = np.insert(buckets, -1, buckets[-1])
        upperBounds = percentilesDf['upper'].values

        return buckets, upperBounds

    def getProbFitFunctionParameters(self):
        param = self.getMediator().fit_function_parameters
        return param["alpha"], param["beta"], param["gamma"], param["maxRate"]

    def getRevenueFitFunctionParameters(self):
        param = self.getMediator().revenue_fit_function_parameters
        return param["alpha"], param["beta"], param["gamma"], param["maxRate"]
