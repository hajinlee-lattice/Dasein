from rulefwk import ColumnRule
from leframework.codestyle import overrides
from collections import Counter
from dataruleutils import calculateOverallConversionRate
from dataruleutils import findDomain
from dataruleutils import getGroupedConversionRate
import pandas as pd

class FrequencyIssue(ColumnRule):

    freqIssuesCols = ['Company', 'LastName', 'FirstName', 'Email']
    freqIssuesPopPercThreshold = 0.01
    pubEmailColName = 'Uses_Public_Email_Provider'
    groupedCountAndConversionRate = {}
    columnsThatFailedTest = {}

    def __init__(self, columns, categoricalColumns, numericalColumns, eventColumn):
        self.columns = columns
        self.catColumn = categoricalColumns
        self.numColumn = numericalColumns
        self.eventColumn = eventColumn

    @overrides(ColumnRule)
    def apply(self, dataFrame, dictOfArguments):
        for columnName, _ in self.columns.iteritems():
            if columnName in dataFrame:
                try:
                    columnType = self.getColumnType(columnName)
                    testResult = self.checkIfColumnHasFrequencyIssues(dataFrame[columnName], dataFrame[self.eventColumn], dataFrame, columnType, columnName)
                    self.columnsThatFailedTest[columnName] = testResult
                except KeyError:
                    # What is default value
                    self.columnsThatFailedTest[columnName] = None

    def getColumnType(self, column):
        if column in self.catColumn:
            return "categorical"
        if column in self.numColumn:
            return "numerical"
        return None

    @overrides
    def explain(self):
        return ""

    def checkIfColumnHasFrequencyIssues(self, dataColumn, eventColumn, dataFrame, colType, columnName):
        colVal = []
        col_cc = Counter()
        overallPositiveCountAndConversionRate = calculateOverallConversionRate(eventColumn)
        groupedCountAndConversionRate = ''
        if columnName in self.freqIssuesCols:
            if columnName == 'Email':
                # print columnName, self.columns, dataColumn.values
                colVal = [findDomain(x) for x in dataColumn.values]
                col_cc = Counter(colVal)
                groupedCountAndConversionRate = getGroupedConversionRate(pd.Series(colVal), eventColumn, overallPositiveCountAndConversionRate)
            else:
                colVal = dataColumn
                col_cc = Counter(dataColumn)
                groupedCountAndConversionRate = getGroupedConversionRate(dataColumn, eventColumn, overallPositiveCountAndConversionRate)
        else:
            colVal = dataColumn
            col_cc = Counter(dataColumn)
            groupedCountAndConversionRate = getGroupedConversionRate(dataColumn, eventColumn, overallPositiveCountAndConversionRate)

        self.groupedCountAndConversionRate.update({columnName: groupedCountAndConversionRate})

        for _, x in col_cc.items():
            if x * 1.0 / len(dataColumn) >= self.freqIssuesPopPercThreshold:
                return True

        return False

    @overrides(ColumnRule)
    def getColumnsToRemove(self):
        return self.columnsThatFailedTest

    def getSummaryPerColumn(self):
        return self.groupedCountAndConversionRate
