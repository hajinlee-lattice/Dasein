from leframework.codestyle import overrides
from rulefwk import ColumnRule

class UncertaintyThreshold(ColumnRule):

    dataProfile = None
    columns = None
    miThreshold = 1.0
    columnsThatFailedTest = {}

    def __init__(self, columns, dataProfile, miThreshold=1.0):
        self.dataProfile = dataProfile
        self.columns = columns
        self.miThreshold = miThreshold

    @overrides(ColumnRule)
    def apply(self, dataFrame, dictOfArguments):
        for columnName, _ in self.columns.iteritems():
            if columnName in dataFrame:
                try:
                    testResult = self.checkMIForColumn(columnName)
                    if testResult:
                        self.columnsThatFailedTest[columnName] = testResult
                except KeyError:
                    # What is default value
                    self.columnsThatFailedTest[columnName] = None

    def checkMIForColumn(self, columnName):
        if columnName in self.dataProfile:
            miForColumn = self.dataProfile[columnName][0][u'uncertaintyCoefficient']

            if miForColumn >= self.miThreshold:
                return True
        else:
            return None

    @overrides(ColumnRule)
    def getColumnsToRemove(self):
        return self.columnsThatFailedTest

    @overrides(ColumnRule)
    def getDescription(self):
        return "Check if UncertaintyCoefficient is greater than " + self.miThreshold + " for column"
