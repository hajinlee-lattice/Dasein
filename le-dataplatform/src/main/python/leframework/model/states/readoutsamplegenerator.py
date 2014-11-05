import logging

from leframework.codestyle import overrides
from leframework.model.state import State
from leframework.util.scoringutil import ScoringUtil

class ReadoutSampleGenerator(State):

    def __init__(self):
        State.__init__(self, "ReadoutSampleGenerator")
        self.logger = logging.getLogger(name='readoutsamplegenerator')

    def insertIntoDataFrame(self, dataFrame, columnName, data, index = 0):
        columns = dataFrame.columns.tolist()
        columns.insert(index, columnName)
        dataFrame[columnName] = data
        return dataFrame[columns]

    @overrides(State)
    def execute(self):
        preTransform = self.mediator.allDataPreTransform.copy()
        postTransform = self.mediator.allDataPostTransform.as_matrix()

        (rows, _) = preTransform.shape

        # Map PreTransform Targets 
        converted = ['N'] * rows
        targetIndex = self.mediator.schema["targetIndex"]
        for i in range(rows):
            if preTransform.iget_value(i, targetIndex) > 0:
                converted[i] = 'Y'

        # Score PostTransform Matrix
        scores = ScoringUtil.score(self.mediator, postTransform, self.logger)

        # Insert Converted
        convertedColumName = "Converted"
        preTransform = self.insertIntoDataFrame(preTransform, convertedColumName, converted)

        # Insert Score
        scoreColumnName = "Score"
        preTransform = self.insertIntoDataFrame(preTransform, scoreColumnName, scores)

        # Sort
        preTransform.sort(scoreColumnName, axis = 0, ascending = False, inplace = True)

        # Extract Rows
        if rows > 2000:
            self.result = preTransform[:1000]
            self.result = self.result.append(preTransform[rows - 1000:])
        else:
            self.result = preTransform

        # Add Result to Mediator
        self.mediator.readoutsample = self.result
