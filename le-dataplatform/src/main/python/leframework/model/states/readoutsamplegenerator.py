import logging
import pandas as pd

from leframework.codestyle import overrides
from leframework.model.state import State
from leframework.util.reservedfieldutil import ReservedFieldUtil

class ReadoutSampleGenerator(State):

    def __init__(self):
        State.__init__(self, "ReadoutSampleGenerator")
        self.logger = logging.getLogger(name='readoutsamplegenerator')

    @overrides(State)
    def execute(self):
        preTransform = self.mediator.allDataPreTransform.copy(deep=False)
        rows = preTransform.shape[0]

        # Sort PreTransform
        scoreColumnName = self.mediator.schema["reserved"]["score"]
        preTransform.sort(scoreColumnName, axis=0, ascending=False, inplace=True)

        # Extract Rows
        if rows > 2000:
            self.result = preTransform[:1000]
            self.result = self.result.append(preTransform[rows - 1000:])
        else:
            self.result = preTransform

        # Map Scores
        percentileScores = []
        if rows > 2000:
            percentileScores = map(lambda e: self.percentile(e, top = True), self.result[scoreColumnName][:1000].as_matrix()) + \
                               map(lambda e: self.percentile(e, top = False), self.result[scoreColumnName][1000:].as_matrix())
        else:
            percentileScores = map(lambda e: self.percentile(e), self.result[scoreColumnName].as_matrix())

        # Update PercentileScores
        percentileScores = pd.Series(percentileScores, index=self.result.index)
        self.result[self.mediator.schema["reserved"]["percentilescore"]].update(percentileScores)

        # Map Targets
        converted = map(lambda e: "Y" if e > 0 else "N", self.result[self.mediator.schema["target"]].as_matrix())

        # Update Converted
        converted = pd.Series(converted, index=self.result.index)
        self.result[self.mediator.schema["reserved"]["converted"]].update(converted)

        # Clean Reserved Fields
        self.cleanReservedFields(self.result)

        # Add Result to Mediator
        self.mediator.readoutsample = self.result

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
