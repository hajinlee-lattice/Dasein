from collections import OrderedDict
import logging
import pandas as pd

from leframework.codestyle import overrides
from leframework.model.state import State
from leframework.util.reservedfieldutil import ReservedFieldUtil

class SampleGenerator(State):

    def __init__(self):
        State.__init__(self, "SampleGenerator")
        self.logger = logging.getLogger(name='samplegenerator')

    @overrides(State)
    def execute(self):
        preTransform = self.mediator.allDataPreTransform.copy(deep=False)

        # Sort PreTransform
        scoreColumnName = self.mediator.schema["reserved"]["score"]
        preTransform.sort(scoreColumnName, axis=0, ascending=False, inplace=True)

        # Generate Samples
        readoutSample = self.generateReadoutSample(preTransform, scoreColumnName)
        (topSample, bottomSample) = self.generateTopAndBottomSamples(readoutSample)

        # Clean Reserved Fields
        self.cleanReservedFields(readoutSample)

        # Add Results to Mediator
        self.mediator.readoutsample = readoutSample
        self.mediator.topsample = topSample
        self.mediator.bottomsample = bottomSample

    def generateReadoutSample(self, preTransform, scoreColumnName):
        rows = preTransform.shape[0]

        # Extract Rows
        if rows > 2000:
            result = preTransform[:1000]
            result = result.append(preTransform[rows - 1000:])
        else:
            result = preTransform

        # Map Scores
        percentileScores = []
        if rows > 2000:
            percentileScores = map(lambda e: self.percentile(e, top = True), result[scoreColumnName][:1000].as_matrix()) + \
                               map(lambda e: self.percentile(e, top = False), result[scoreColumnName][1000:].as_matrix())
        else:
            percentileScores = map(lambda e: self.percentile(e), result[scoreColumnName].as_matrix())

        # Update PercentileScores
        percentileScores = pd.Series(percentileScores, index=result.index)
        result[self.mediator.schema["reserved"]["percentilescore"]].update(percentileScores)

        # Map Targets
        converted = map(lambda e: "Y" if e > 0 else "N", result[self.mediator.schema["target"]].as_matrix())

        # Update Converted
        converted = pd.Series(converted, index=result.index)
        result[self.mediator.schema["reserved"]["converted"]].update(converted)

        return result

    def percentile(self, score, top = True):
        buckets = self.mediator.percentileBuckets
        maxScore = buckets[0]["MaximumScore"] if len(buckets) != 0 else 1

        if score >= maxScore:
            return 100
        else:
            order = 1 if top else -1
            for bucket in buckets[::order]:
                if score >= bucket["MinimumScore"] and score < bucket["MaximumScore"]:
                    return bucket["Percentile"]

        return None

    def cleanReservedFields(self, dataFrame):
        columns = dataFrame.columns.tolist()
        reserved = self.mediator.schema["reserved"]

        for key in reserved.keys():
            index = columns.index(reserved[key])
            columns[index] = ReservedFieldUtil.extractDisplayName(columns[index])

        dataFrame.columns = columns

    def generateTopAndBottomSamples(self, readoutSample):
        '''
        Good Enough (For Now)
        '''
        topSample = []
        for i in xrange(10):
            lead = OrderedDict()
            lead["Company"] = "Company " + str(i)
            lead["Contact"] = "Contact " + str(i)
            lead["Converted"] = False if i in [2, 6, 7] else True
            lead["Score"] = 100
            topSample.append(lead)

        bottomSample = []
        for i in xrange(10):
            lead = OrderedDict()
            lead["Company"] = "Company " + str(10 + i)
            lead["Contact"] = "Contact " + str(10 + i)
            lead["Converted"] = False
            lead["Score"] = 1
            bottomSample.append(lead)

        return (topSample, bottomSample)
