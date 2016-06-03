from rulefwk import ColumnRule
from leframework.codestyle import overrides

class PopulatedRowCount(ColumnRule):

    columnsThatFailedTest = {}
    threshold = 0.005

    def __init__(self, columns, threshold=0.005):
        self.threshold = 0.005
        self.columns = columns

    @overrides(ColumnRule)
    def apply(self, dataFrame, dictOfArguments):
        for columnName, _ in self.columns.iteritems():
            if columnName in dataFrame:
                try:
                    testResult = self.checkColumnAgainstPopulatedRowCount(dataFrame[columnName], columnName)
                    self.columnsThatFailedTest[columnName] = testResult
                except KeyError:
                    # What is default value
                    self.columnsThatFailedTest[columnName] = None

    def checkColumnAgainstPopulatedRowCount(self, dataColumn, columnName):
        return dataColumn.count() < self.threshold;

    @overrides(ColumnRule)
    def getColumnsToRemove(self):
        return self.columnsThatFailedTest

    def getSummaryPerColumn(self):
        return self.columnsThatFailedTest

