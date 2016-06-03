from dataruleutils import isCategorical
from leframework.codestyle import overrides
from rulefwk import ColumnRule


class CountUniqueValueRule(ColumnRule):
    columnsThatFailedTest = {}
    uniqueCountThreshold = 200

    def __init__(self, columns, categoricalColumns, numericalColumns, eventColumn, uniqueCountThreshold=200):
        self.columns = columns
        self.uniqueCountThreshold = uniqueCountThreshold
        self.catColumn = categoricalColumns
        self.numColumn = numericalColumns
        self.eventColumn = eventColumn

    def getColumnType(self, column):
        if column in self.catColumn:
            return "categorical"
        if column in self.numColumn:
            return "numerical"
        return None

    @overrides(ColumnRule)
    def getDescription(self):
        return "Check if the number of unique values in a categorical column are greater than threshold(Set to " + self.uniqueCountThreshold + ")"

    @overrides(ColumnRule)
    def apply(self, dataFrame, dictOfArguments):
        for columnName, _ in self.columns.iteritems():
            if columnName in dataFrame:
                try:
                    columnType = self.getColumnType(columnName)
                    testResult = self.checkColumnForUniqueValues(dataFrame[columnName], dataFrame[self.eventColumn], columnType, columnName)
                    self.columnsThatFailedTest[columnName] = testResult
                except KeyError:
                    # What is default value
                    self.columnsThatFailedTest[columnName] = None

    def checkColumnForUniqueValues(self, dataColumn, eventColumn, columnType, columnName):
        if isCategorical(columnType):
            if len(dataColumn.unique()) > self.uniqueCountThreshold:
                return True
            else:
                return False
        else:
            return None

    def getResults(self):
        return self.columnsThatFailedTest

    @overrides(ColumnRule)
    def getColumnsToRemove(self):
        return self.columnsThatFailedTest

    def getSummaryPerColumn(self):
        return self.columnsThatFailedTest
