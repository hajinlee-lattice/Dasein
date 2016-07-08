from rulefwk import ColumnRule
from leframework.codestyle import overrides
import pandas as pd
from dataruleutilsDS import convertCleanDataFrame

class LowCoverageDS(ColumnRule):

    def __init__(self, columns, categoricalColumns, numericalColumns, lowcoverageThreshold = 0.95):
        self.columns = columns.keys()
        self.catColumn = set(categoricalColumns.keys())
        self.numColumn = set(numericalColumns.keys())
        self.lowcoverageThreshold = lowcoverageThreshold
        self.columnsInfo = {}

    def createInternalData(self, dataFrame):
        self.data = convertCleanDataFrame(self.columns, dataFrame, self.catColumn, self.numColumn)

    @overrides(ColumnRule)
    def apply(self, dataFrame, dictOfArguments):
        self.createInternalData(dataFrame)

        for columnName in self.columns:
            try:
                testResult = self.getLowcoverage(columnName)
                self.columnsInfo[columnName] = testResult
            except KeyError:
                # What is default value
                self.columnsInfo[columnName] = None

    @overrides
    def getDescription(self):
        return "Check if column has too many missing values"

    def getLowcoverage(self, columnName):
        colVal = self.data[self.columns.index(columnName)]
        nullRate = len([x for x in colVal if x == '' or pd.isnull(x)])*1.0/len(colVal)
        if nullRate >= self.lowcoverageThreshold:
            return (True, nullRate)
        else:
            return (False, nullRate)

    def getColumnsInfo(self):
        return self.columnsInfo

    @overrides(ColumnRule)
    def getColumnsToRemove(self):
        return {key: val[0] for key, val in self.columnsInfo.items()}
