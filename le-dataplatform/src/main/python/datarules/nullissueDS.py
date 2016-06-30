from rulefwk import ColumnRule
from leframework.codestyle import overrides
from dataruleutilsDS import getRate
from dataruleutilsDS import getColVal
from dataruleutilsDS import getGroupedRate
from dataruleutilsDS import ismissing
from dataruleutilsDS import convertCleanDataFrame
import pandas as pd

class NullIssueDS(ColumnRule):

    def __init__(self, columns, categoricalColumns, numericalColumns, eventColumn, numBucket = 20, nullIssue_toppopPerc_threshold = 0.1, nullIssue_lift_threshold = 1.5):
        self.columns = columns.keys()
        self.catColumn = set(categoricalColumns.keys())
        self.numColumn = set(numericalColumns.keys())
        self.columnTypes = [self.getColumnType(col) for col in self.columns]
        self.nullIssue_toppopPerc_threshold = nullIssue_toppopPerc_threshold
        self.nullIssue_lift_threshold = nullIssue_lift_threshold
        self.eventColName = eventColumn
        self.numBucket = numBucket
        self.columnsInfo = {}
        self.groupedCountAndConversionRate = {}

    def getColumnType(self, column):
        if column in self.catColumn:
            return "cat"
        if column in self.numColumn:
            return "num"
        return None

    def createInternalData(self, dataFrame):
        self.eventCol = [float(x) for x in dataFrame[self.eventColName].tolist()]
        self.data = convertCleanDataFrame(self.columns, dataFrame, self.catColumn, self.numColumn)

    @overrides(ColumnRule)
    def apply(self, dataFrame, dictOfArguments):
        self.createInternalData(dataFrame)

        for columnName, colType in zip(self.columns, self.columnTypes):
            print(columnName, colType)
            try:
                testResult = self.get_nullIssues(columnName, colType, self.eventCol)
                self.columnsInfo[columnName] = testResult
            except KeyError:
                # What is default value
                self.columnsInfo[columnName] = None

    @overrides
    def explain(self):
        return "Check if column is overly positive predictive from missing values"

    #input:
        # colVal: list of feature
        # colType: type of feature: numerical or categorical
        # eventCol: list of event (0s, 1s)
    #output:
        # dict = {colName: (True/False, populateRate, lift)}
    def get_nullIssues(self, columnName, colType, eventCol):

        colVal = self.data[self.columns.index(columnName)]

        # get the over-all rate
        cntRate_overall = (len(eventCol), getRate(eventCol))
        # get the column of values
        colVal = getColVal(colVal, colType, self.numBucket)
        # get the rate with respect to each bucket
        cntRate_grouped = getGroupedRate(colVal, eventCol, cntRate_overall)

        self.groupedCountAndConversionRate.update({columnName: cntRate_grouped})

        # get the rate with respect to missing values
        cntRate_Null = [[x, y[0], y[1]]  for x, y in cntRate_grouped.items() if ismissing(x, colType)]

        if len(cntRate_Null) == 0:
            return (False, 0, 0)
        else:
            cntRate_Null = cntRate_Null[0]

        # get the rate with respect to non-missing values
        cntRate_nonNull = [[x, y[0], y[1]] for x, y in cntRate_grouped.items() if not ismissing(x, colType)]

        # sort the buckets of non-missing values with respect to its rate
        cntRate_nonNull_sorted = sorted(cntRate_nonNull, key = lambda x: x[2], reverse = True)

        # start from the bucket with highest rate to the bucket with lowest rate, find the cutoff value based on the threshhold
        pop = 0
        event = 0
        for cntRate in cntRate_nonNull_sorted:
            if pop <= cntRate_overall[0]*self.nullIssue_toppopPerc_threshold:
                pop += cntRate[1]
                event += cntRate[1]*cntRate[2]
            else:
                break
        # if (r_nm/r_0 - 1) < (r_m/r_0 - 1)*threshold where r_nm is the rte from non-mising values, r_m is from missing values, r_0 is overall rate, the feature has null issues
        if (event/pop - cntRate_overall[1]) < self.nullIssue_lift_threshold*(cntRate_Null[2] - cntRate_overall[1]):
            return (True, cntRate_Null[1]*1.0/cntRate_overall[0], cntRate_Null[2]/cntRate_overall[1])
        else:
            return (False, cntRate_Null[1]*1.0/cntRate_overall[0], cntRate_Null[2]/cntRate_overall[1])

    def getColumnsInfo(self):
        return {key: val for key, val in self.columnsInfo.items()}

    @overrides(ColumnRule)
    def getColumnsToRemove(self):
        return {key: val[0] for key, val in self.columnsInfo.items()}

    def getSummaryPerColumn(self):
        return self.groupedCountAndConversionRate
