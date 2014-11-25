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
        postTransform = self.mediator.allDataPostTransform.as_matrix()
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

        # Map Targets (+1 offset due to score insertion)
        targetColumn = self.result.columns.tolist()[self.mediator.schema["targetIndex"] + 1]
        converted = map(lambda e: "Y" if e > 0 else "N", self.result[targetColumn].as_matrix())

        # Insert Converted (+1 offset due to score insertion)
        convertedColumName = "Converted"
        self.result = PandasUtil.insertIntoDataFrame(self.result, convertedColumName, converted, 1)

        # Shift Readouts (+2 offset due to score/converted insertion)
        tailCount = len(nonScoringTargets)
        for column in nonScoringTargets[::-1]:
            if column in readouts:
                (self.result, moved) = PandasUtil.moveTailColumn(self.result, tailCount, column, 2)
            else:
                (self.result, moved) = PandasUtil.moveTailColumn(self.result, tailCount, column)
            if moved: tailCount = tailCount - 1

        # Add Result to Mediator
        self.mediator.readoutsample = self.result
