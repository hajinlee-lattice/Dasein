from rulefwk import ColumnRule, RuleResults
from leframework.codestyle import overrides
import pandas as pd
from dataruleutilsds import convertCleanDataFrame

class UniqueValueCountDS(ColumnRule):

    def __init__(self, columns, categoricalColumns, numericalColumns, uniquevaluecountThreshold = 200):
        self.columns = columns.keys()
        self.catColumn = set(categoricalColumns.keys())
        self.numColumn = set(numericalColumns.keys())
        self.columnTypes = [self.getColumnType(col) for col in self.columns]
        self.uniquevaluecountThreshold = uniquevaluecountThreshold
        self.columnsInfo = {}

    def createInternalData(self, dataFrame):
        self.data = convertCleanDataFrame(self.columns, dataFrame, self.catColumn, self.numColumn)

    def getColumnType(self, column):
        if column in self.catColumn:
            return "cat"
        if column in self.numColumn:
            return "num"
        return None

    @overrides(ColumnRule)
    def apply(self, dataFrame, dictOfArguments):
        self.createInternalData(dataFrame)
        for columnName, colType in zip(self.columns, self.columnTypes):
            try:
                testResult = self.getUniquevaluecount(columnName, colType)
                self.columnsInfo[columnName] = testResult
            except KeyError:
                # What is default value
                self.columnsInfo[columnName] = None

    @overrides
    def getDescription(self):
        return "Check if categorical column has too many values from one value or numerical column has too many non-null values from one value"

    def getUniquevaluecount(self, columnName, colType):
        if colType == 'cat':
            cc = len(set(self.data[self.columns.index(columnName)]))
            if cc >= self.uniquevaluecountThreshold:
                return (True, cc)
            else:
                return (False, cc)
        return None

    @overrides(ColumnRule)
    def getColumnsToRemove(self):
        return {key: False if val is None else val[0] for key, val in self.columnsInfo.items()}

    @overrides(ColumnRule)
    def getConfParameters(self):
        return { 'uniquevaluecountThreshold':self.uniquevaluecountThreshold }

    @overrides(ColumnRule)
    def getResults(self):
        results = {}
        for col, testResult in self.columnsInfo.iteritems():
            rr = None
            if testResult is None:
                rr = RuleResults(True, 'Column not checked', {})
            else:
                (isFailed, cc) = testResult
                rr = RuleResults(not isFailed, 'Number of distinct values: {0}'.format(cc), {'distinctValues':cc})
            results[col] = rr

        return results


    def getColumnsInfo(self):
        return self.columnsInfo
