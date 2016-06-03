from rulefwk import ColumnRule
from leframework.codestyle import overrides
from dataruleutils import calculateOverallConversionRate
from dataruleutils import getGroupedConversionRate
from dataruleutils import isCategorical, isNumerical, calculateConversionRate, calculateSignificanceOfSubPopulation
from math import isnan
import numpy as np
import pandas as pd

class FutureInformation(ColumnRule):

    futureInfoCols = {'Title_Length' : 1, 'CompanyName_Length': 1, 'Phone_Entropy':  float('nan'), 'CompanyName_Entropy': float('nan'), 'LeadSouce': None}
    futureInfoPopPercThreshold = 0.01
    futureInfoLiftThreshold = 1.1
    futureInfoSigThreshold = 2
    columnsThatFailedTest = {}
    groupedCountAndConversionRate = {}

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
                    testResult = self.checkIfColumnHasFutureInformation(dataFrame[columnName], dataFrame[self.eventColumn], columnType, columnName)
                    self.columnsThatFailedTest[columnName] = testResult
                except KeyError:
                    # What is default value
                    self.columnsThatFailedTest[columnName] = None

    @overrides
    def explain(self):
        return "Check if Conversion Rate and Significance are greater than threshold"

    def getColumnType(self, column):
        if column in self.catColumn:
            return "categorical"
        if column in self.numColumn:
            return "numerical"
        return None

    def checkIfColumnHasFutureInformation(self, dataColumn, eventColumn, colType, columnName):
        overallPositiveCountAndConversionRate = calculateOverallConversionRate(eventColumn)
        dataColumn = dataColumn.replace(np.nan, "np.nan")
        groupedCountAndConversionRate = getGroupedConversionRate(dataColumn, eventColumn, overallPositiveCountAndConversionRate)
        self.groupedCountAndConversionRate.update({columnName: groupedCountAndConversionRate})

        if isCategorical(colType):
            for _, x in groupedCountAndConversionRate.items():
                if x[1] * 1.0 / overallPositiveCountAndConversionRate[1] >= self.futureInfoLiftThreshold and \
                        x[2] >= self.futureInfoSigThreshold:
                    return True
                else:
                    return False
        elif isNumerical(colType):
            valueToCheck = self.futureInfoCols[columnName]
            if valueToCheck == 1:
                valColChk = [x[1] for x in zip(dataColumn, eventColumn) if x[0] in [valueToCheck, str(valueToCheck)]]
                r = calculateConversionRate(pd.Series(valColChk))
                sig = calculateSignificanceOfSubPopulation((len(valColChk), r), overallPositiveCountAndConversionRate)
                if r * 1.0 / overallPositiveCountAndConversionRate[1] >= self.futureInfoLiftThreshold and \
                    sig >= self.futureInfoSigThreshold:
                    return True
                else:
                    return False
            elif isnan(valueToCheck) or valueToCheck in ['', None]:
                if 0 in groupedCountAndConversionRate.keys() and \
                    groupedCountAndConversionRate['np.nan'][1] * 1.0 / overallPositiveCountAndConversionRate[1] >= self.futureInfoLiftThreshold and \
                    groupedCountAndConversionRate['np.nan'][2] >= self.futureInfoSigThreshold:
                    valColChk = [x[1] for x in zip(dataColumn, eventColumn) if x[0] in ['', None, 0]]
                    r = calculateConversionRate(pd.Series(valColChk))
                    sig = calculateSignificanceOfSubPopulation((len(valColChk), r), overallPositiveCountAndConversionRate)
                    return True
                else:
                    return False
        else:
            return None

    @overrides(ColumnRule)
    def getColumnsToRemove(self):
        return self.columnsThatFailedTest

    def getSummaryPerColumn(self):
        return self.groupedCountAndConversionRate
