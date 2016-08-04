from rulefwk import RowRule, RuleResults
from leframework.codestyle import overrides
from dataruleutilsds import getRate
from dataruleutilsds import getGroupedRate
from dataruleutilsds import convertCleanDataFrame
from dataruleutilsds import selectIdColumn
import pandas as pd

class HighlyPredictiveSmallPopulationDS(RowRule):

    def __init__(self, columns, categoricalColumns, numericalColumns, eventColumn, highlyPredictiveSmallPopulationLiftThreshold = 3, highlyPredictiveSmallPopulationPopThreshold = 0.01):
        self.columns = columns.keys()
        self.catColumn = set(categoricalColumns.keys())
        self.numColumn = set(numericalColumns.keys())
        self.columnTypes = [self.getColumnType(col) for col in self.columns]
        self.highlyPredictiveSmallPopulationLiftThreshold = highlyPredictiveSmallPopulationLiftThreshold
        self.highlyPredictiveSmallPopulationPopThreshold = highlyPredictiveSmallPopulationPopThreshold
        self.eventColName = eventColumn
        self.columnsInfo = {}
        self.rowsToRemove = {}

    def getColumnType(self, column):
        if column in self.catColumn:
            return "cat"
        if column in self.numColumn:
            return "num"
        return None

    def createInternalData(self, dataFrame):
        self.eventCol = [float(x) for x in dataFrame[self.eventColName].tolist()]
        self.data = convertCleanDataFrame(self.columns, dataFrame, self.catColumn, self.numColumn)

    @overrides(RowRule)
    def apply(self, dataFrame, dictOfArguments):
        self.idCol = dataFrame[selectIdColumn(dataFrame)].tolist()

        self.createInternalData(dataFrame)

        for columnName, colType in zip(self.columns, self.columnTypes):
            try:
                testResult = self.getHighlyPredictiveSmallPopulation(columnName, colType, self.eventCol)
                if len(testResult) > 0:
                    self.columnsInfo[columnName] = testResult
                    self.updateRowsToRemove(columnName, colType, testResult)

            except KeyError:
                # What is default value
                self.columnsInfo[columnName] = None

    def getHighlyPredictiveSmallPopulation(self, columnName, colType, eventCol):
        colVal = self.data[self.columns.index(columnName)]

        cntRateOverall = (len(eventCol), getRate(eventCol))

        highlyPositivelyPredictiveSmallPopulation = []

        if colType == 'cat':

            cntRateGrouped = getGroupedRate(colVal, eventCol)

            if len(cntRateGrouped.keys()) <= 200:
                for key, x in cntRateGrouped.items():
                    if x[0]*1.0/cntRateOverall[0] <= self.highlyPredictiveSmallPopulationPopThreshold and x[1]*1.0/cntRateOverall[1] >= self.highlyPredictiveSmallPopulationLiftThreshold:
                        highlyPositivelyPredictiveSmallPopulation.append([colType, key, x[0], x[0]*x[1], x[1]/cntRateOverall[1]])
        # for numerical variable, only null value need to be looked at. rows with null values are marked at bucket 0
        elif colType == 'num':
            colEventTup = [x for x in zip(colVal,eventCol) if pd.isnull(x[0])]
            if len(colEventTup) > 0:
                colNull = [x[0] for x in colEventTup]
                colNullrate = getRate([x[1] for x in colEventTup])

                if len(colNull)*1.0/cntRateOverall[0] <= self.highlyPredictiveSmallPopulationPopThreshold and colNullrate*1.0/cntRateOverall[1] >= self.highlyPredictiveSmallPopulationLiftThreshold:
                    highlyPositivelyPredictiveSmallPopulation.append([colType, 'nan', len(colNull), len(colNull)*colNullrate, colNullrate/cntRateOverall[1]])

        return highlyPositivelyPredictiveSmallPopulation

    def updateRowsToRemove(self, columnName, colType, highlyPositivelyPredictiveSmallPopulation):
        for group in highlyPositivelyPredictiveSmallPopulation:
            colVal = self.data[self.columns.index(columnName)]
            colType = group[0]
            key = group[1]
            if colType == 'cat':
                idGroup = [x[1] for x in zip(colVal, self.idCol) if x[0] == key]
            elif colType == 'num' and key == 'nan':
                idGroup = [x[1] for x in zip(colVal, self.idCol) if pd.isnull(x[0])]
            else:
                idGroup = []
            for id in idGroup:
                if id in self.rowsToRemove:
                    self.rowsToRemove[id].append(columnName)
                else:
                    self.rowsToRemove[id] = [columnName]

    @overrides(RowRule)
    def getRowsToRemove(self):
        return self.rowsToRemove

    @overrides(RowRule)
    def getConfParameters(self):
        return { 'highlyPredictiveSmallPopulationLiftThreshold':self.highlyPredictiveSmallPopulationLiftThreshold, \
                 'highlyPredictiveSmallPopulationPopThreshold':self.highlyPredictiveSmallPopulationPopThreshold }

    @overrides(RowRule)
    def getResults(self):
        results = {}
        for rowid, columns in self.rowsToRemove.iteritems():
            rr = RuleResults(False, 'Row failed', {})
            results[rowid] = rr
        return results

    def getColumnsInfo(self):
        return self.columnsInfo

    @overrides(RowRule)
    def getDescription(self):
        return "given the lift of each distinctive feature value in column \
         and the size of the population, decide if the corresponding rows should be deleted"
