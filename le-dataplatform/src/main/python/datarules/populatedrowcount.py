from rulefwk import ColumnRule
from leframework.codestyle import overrides

class PopulatedRowCount(ColumnRule):

    def __init__(self, columns, threshold=0.0):
        self.thresholdForPercentageOfNulls = threshold
        self.columns = columns
        self.columnsThatFailedTest = {}

    @overrides(ColumnRule)
    def apply(self, dataFrame, dictOfArguments):
        self.columnsThatFailedTest = {}
        for columnName, _ in self.columns.iteritems():
            if columnName in dataFrame:
                try:
                    testResult = self.checkColumnAgainstPopulatedRowCount(dataFrame[columnName], columnName)
                    if testResult:
                        self.columnsThatFailedTest[columnName] = testResult
                except KeyError:
                    # What is default value
                    self.columnsThatFailedTest[columnName] = None

    def checkColumnAgainstPopulatedRowCount(self, dataColumn, columnName):
        percentageOfNulls = 1.0 - (dataColumn.count() / float(len(dataColumn)))
        thresholdForNulls = 1.0 - self.thresholdForPercentageOfNulls

        return percentageOfNulls >= thresholdForNulls

    @overrides(ColumnRule)
    def getColumnsToRemove(self):
        return self.columnsThatFailedTest

    def getSummaryPerColumn(self):
        return self.columnsThatFailedTest

    @overrides(ColumnRule)
    def getDescription(self):
        return "Check if column has more than threshold% of Nulls. For a threshold of 0.0, rule would return True \
            if all rows were empty, rule would return False otherwise."


