from rulefwk import ColumnRule
from leframework.codestyle import overrides
from dataruleutils import calculateOverallConversionRate, getGroupedConversionRate, isCategorical, isNumerical

class HighlyPredictiveSmallPopulation(ColumnRule):

    columnsThatFailedTest = {}
    highlyPositivelyPredictiveSmallPopulationPopPercThreshold = 0.01
    highlyPositivelyPredictiveSmallPopulationLiftThreshold = 1.2
    groupedCountAndConversionRate = {}

    def __init__(self, columns, categoricalColumns, numericalColumns, eventColumn, highlyPositivelyPredictiveSmallPopulationPopPercThreshold=0.01,
                highlyPositivelyPredictiveSmallPopulationLiftThreshold=1.2):
        self.columns = columns
        self.catColumn = categoricalColumns
        self.numColumn = numericalColumns
        self.eventColumn = eventColumn
        self.highlyPositivelyPredictiveSmallPopulationPopPercThreshold = highlyPositivelyPredictiveSmallPopulationPopPercThreshold
        self.highlyPositivelyPredictiveSmallPopulationLiftThreshold = highlyPositivelyPredictiveSmallPopulationLiftThreshold
        pass

    def explain(self):
        return "given the conversion rate of each distinctive feature value in column \
         and the overall conversion rate, decide if the corresponding rows should be deleted"

    def getColumnType(self, column):
        if column in self.catColumn:
            return "categorical"
        if column in self.numColumn:
            return "numerical"
        return None

    @overrides(ColumnRule)
    def apply(self, dataFrame, dictOfArguments):
        for column, _ in self.columns.iteritems():
            if column in dataFrame:
                try:
                    columnType = self.getColumnType(column)
                    testResult = self.checkIfColumnIsHighlyPredictiveSmallPopulation(dataFrame[column], dataFrame[self.eventColumn], columnType, column)
                    self.columnsThatFailedTest[column] = testResult
                except KeyError:
                    # What is default value
                    self.columnsThatFailedTest[column] = None

    def checkIfColumnIsHighlyPredictiveSmallPopulation(self, dataColumn, eventColumn, colType, columnName):
        overallPositiveCountAndConversionRate = calculateOverallConversionRate(eventColumn)
        groupedCountAndConversionRate = getGroupedConversionRate(dataColumn, eventColumn, overallPositiveCountAndConversionRate)
        self.groupedCountAndConversionRate.update({columnName: groupedCountAndConversionRate})

        if isCategorical(colType):
            for _, x in groupedCountAndConversionRate.items():
                if x[0] * 1.0 / overallPositiveCountAndConversionRate[0] <= self.highlyPositivelyPredictiveSmallPopulationPopPercThreshold and \
                   x[1] * 1.0 / overallPositiveCountAndConversionRate[1] >= self.highlyPositivelyPredictiveSmallPopulationLiftThreshold:
                    return True
            return False
        elif isNumerical(colType):
            if 0 in groupedCountAndConversionRate.keys() and \
            groupedCountAndConversionRate[0][0] * 1.0 / overallPositiveCountAndConversionRate[0] <= self.highlyPositivelyPredictiveSmallPopulationPopPercThreshold and \
            groupedCountAndConversionRate[0][1] * 1.0 / overallPositiveCountAndConversionRate[1] >= self.highlyPositivelyPredictiveSmallPopulationLiftThreshold:
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
