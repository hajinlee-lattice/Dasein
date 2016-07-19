from rulefwk import ColumnRule, RuleResults
from leframework.codestyle import overrides
from dataruleutilsds import convertCleanDataFrame
import pandas as pd
from collections import Counter

class PopulatedRowCountDS(ColumnRule):

    def __init__(self, columns, categoricalColumns, numericalColumns, populatedrowcountCatThreshold = 0.98,populatedrowcountNumThreshold = 0.98, numChklist = [0, 1,-1]):
        self.columns = columns.keys()
        self.catColumn = set(categoricalColumns.keys())
        self.numColumn = set(numericalColumns.keys())
        self.columnTypes = [self.getColumnType(col) for col in self.columns]
        self.populatedrowcountCatThreshold = populatedrowcountCatThreshold
        self.populatedrowcountNumThreshold = populatedrowcountNumThreshold
        self.numChklist = numChklist
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
                testResult = self.getPopulatedrowcount(columnName, colType)
                self.columnsInfo[columnName] = testResult
            except KeyError:
                # What is default value
                self.columnsInfo[columnName] = None

    @overrides
    def getDescription(self):
        return "Check if categorical column has too many values from one value or numerical column has too many non-null values from one value"

    def getPopulatedrowcount(self, columnName, colType):
        colVal = self.data[self.columns.index(columnName)]
        lenData = len(colVal)
        if colType == 'cat':
            cc = Counter(colVal)
            for k, v in cc.items():
                if v*1.0/lenData >= self.populatedrowcountCatThreshold:
                    return {k:v*1.0/lenData}
        elif colType == 'num':
            lenData = len([x for x in colVal if not pd.isnull(x)])
            if lenData > 0:
                for numChkval in self.numChklist:
                    colValChk = len([x for x in colVal if x == numChkval])
                    if colValChk*1.0/lenData >= self.populatedrowcountNumThreshold:
                        return {numChkval: colValChk*1.0/lenData}
        return None

    @overrides(ColumnRule)
    def getColumnsToRemove(self):
        return {key: False if val is None else True for key, val in self.columnsInfo.items()}

    @overrides(ColumnRule)
    def getConfParameters(self):
        return { 'populatedrowcountCatThreshold':self.populatedrowcountCatThreshold, \
                 'populatedrowcountNumThreshold':self.populatedrowcountNumThreshold, \
                 'numChklist':self.numChklist }

    @overrides(ColumnRule)
    def getResults(self):
        results = {}
        for col, testResult in self.columnsInfo.iteritems():
            rr = None
            if testResult is None:
                rr = RuleResults(True, 'No single value has population percentage above threshold', {})
                results[col] = rr
            else:
                for val, frac in testResult.iteritems():
                    rr = RuleResults(False, 'Value \'{0}\' has population percentage {1:.2%}'.format(val, frac), {'percentage':frac*100.0})
                    results[col] = rr
        return results

    def getColumnsInfo(self):
        return self.columnsInfo
