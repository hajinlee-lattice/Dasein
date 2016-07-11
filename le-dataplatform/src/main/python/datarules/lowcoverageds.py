from rulefwk import ColumnRule
from leframework.codestyle import overrides
import pandas as pd
from dataruleutilsds import convertCleanDataFrame


class LowCoverageDS(ColumnRule):

    def __init__(self, columns, categoricalColumns, numericalColumns, lowcoverage_threshold = 0.95):
        self.columns = columns.keys()
        self.catColumn = set(categoricalColumns.keys())
        self.numColumn = set(numericalColumns.keys())
        self.lowcoverage_threshold = lowcoverage_threshold
        self.columnsInfo = {}

    def createInternalData(self, dataFrame):
        self.data = convertCleanDataFrame(self.columns, dataFrame, self.catColumn, self.numColumn)

    @overrides(ColumnRule)
    def apply(self, dataFrame, dictOfArguments):
        self.createInternalData(dataFrame)

        for columnName in self.columns:
            try:
                testResult = self.get_lowcoverage(columnName)
                self.columnsInfo[columnName] = testResult
            except KeyError:
                # What is default value
                self.columnsInfo[columnName] = None

    @overrides
    def explain(self):
        return "Check if column has too many missing values"

    #input:
        # colVal: list of feature
    #output:
        # dict = {colName: (True/False, populateRate)}

    def get_lowcoverage(self, columnName):
        colVal = self.data[self.columns.index(columnName)]
        null_rate = len([x for x in colVal if x == '' or pd.isnull(x)])*1.0/len(colVal)
        if null_rate >= self.lowcoverage_threshold:
            return (True, null_rate)
        else:
            return (False, null_rate)

    def getColumnsInfo(self):
        return self.columnsInfo

    @overrides(ColumnRule)
    def getColumnsToRemove(self):
        return {key: val[0] for key, val in self.columnsInfo.items()}
