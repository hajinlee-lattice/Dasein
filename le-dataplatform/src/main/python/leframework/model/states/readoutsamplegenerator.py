import logging

from leframework.codestyle import overrides
from leframework.model.state import State
from leframework.util.scoringutil import ScoringUtil
from leframework.util.pandasutil import PandasUtil

class ReadoutSampleGenerator(State):

    def __init__(self):
        State.__init__(self, "ReadoutSampleGenerator")
        self.logger = logging.getLogger(name='readoutsamplegenerator')

    @overrides(State)
    def execute(self):
        preTransform = self.mediator.allDataPreTransform.copy()
        postTransform = self.mediator.allDataPostTransform
        nonScoringTargets = self.mediator.schema["nonScoringTargets"]
        readouts = self.mediator.schema["readouts"]

        (rows, _) = preTransform.shape

        # Score PostTransform Matrix
        scores = ScoringUtil.score(self.mediator, postTransform, self.logger)

        # Insert Score
        scoreColumnName = "Score"
        preTransform = PandasUtil.insertIntoDataFrame(preTransform, scoreColumnName, scores)

        # Sort
        preTransform.sort(scoreColumnName, axis = 0, ascending = False, inplace = True)

        # Extract Rows
        if rows > 2000:
            self.result = preTransform[:1000]
            self.result = self.result.append(preTransform[rows - 1000:])
        else:
            self.result = preTransform

        # Map Scores
        percentileScores = []
        if rows > 2000:
            percentileScores = map(lambda e: self.percentile(e, top = True), self.result["Score"][:1000].as_matrix()) + \
                               map(lambda e: self.percentile(e, top = False), self.result["Score"][1000:].as_matrix())
        else:
            percentileScores = map(lambda e: self.percentile(e), self.result["Score"].as_matrix())

        # Insert PercentileScore
        percentileScoreColumnName = "PercentileScore"
        self.result = PandasUtil.insertIntoDataFrame(self.result, percentileScoreColumnName, percentileScores)

        # Map Targets
        converted = map(lambda e: "Y" if e > 0 else "N", self.result[self.mediator.schema["target"]].as_matrix())

        # Insert Converted (+2 offset due to score insertions)
        convertedColumName = "Converted"
        self.result = PandasUtil.insertIntoDataFrame(self.result, convertedColumName, converted, 2)

        # Shift Readouts (+3 offset due to score/converted insertions)
        tailCount = len(nonScoringTargets)
        for column in nonScoringTargets[::-1]:
            if column in readouts:
                (self.result, moved) = PandasUtil.moveTailColumn(self.result, tailCount, column, 3)
            else:
                (self.result, moved) = PandasUtil.moveTailColumn(self.result, tailCount, column)
            if moved: tailCount = tailCount - 1

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
