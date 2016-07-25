from rulefwk import ColumnRule, RuleResults
from leframework.codestyle import overrides
from dataruleutilsds import getRate
from dataruleutilsds import getColVal
from dataruleutilsds import getGroupedRate
from dataruleutilsds import convertCleanDataFrame
import pandas as pd

class NullIssueDS(ColumnRule):

    def __init__(self, columns, categoricalColumns, numericalColumns, eventColumn, numBucket = 20, nullIssueLiftThreshold = 1.2, nullIssueToppopPercThreshold = 0.1, nullIssuePredictiveThreshold = 1.5):
        self.columns = columns.keys()
        self.catColumn = set(categoricalColumns.keys())
        self.numColumn = set(numericalColumns.keys())
        self.columnTypes = [self.getColumnType(col) for col in self.columns]
        self.nullIssueToppopPercThreshold = nullIssueToppopPercThreshold
        self.nullIssueLiftThreshold = nullIssueLiftThreshold
        self.nullIssuePredictiveThreshold = nullIssuePredictiveThreshold
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
            try:
                testResult = self.getNullIssues(columnName, colType, self.eventCol)
                self.columnsInfo[columnName] = testResult
            except KeyError:
                # What is default value
                self.columnsInfo[columnName] = None

    @overrides
    def getDescription(self):
        return "Check if column is overly positive predictive from missing values"

    def isMissingAfterBucketing(self, val, type):
        if type == 'cat' and (val == '' or pd.isnull(val)):
            return True
        elif type == 'num' and val == 0:
            return True
        else:
            return False

    def getNullIssues(self, columnName, colType, eventCol):

        colVal = self.data[self.columns.index(columnName)]

        # get the over-all rate
        cntRateOverall = (len(eventCol), getRate(eventCol))

        # get the column of values
        colVal = getColVal(colVal, colType, self.numBucket)

        # get the rate with respect to each bucket
        cntRateGrouped = getGroupedRate(colVal, eventCol)

        self.groupedCountAndConversionRate.update({columnName: cntRateGrouped})

        # get the rate with respect to missing values
        cntRateNull = [[x, y[0], y[1]]  for x, y in cntRateGrouped.items() if self.isMissingAfterBucketing(x, colType)]

        if len(cntRateNull) == 0:
            return (False, 0, 0, 1.0, 1.0, -1, 0, cntRateOverall[1])
        else:
            cntRateNull = cntRateNull[0]

        popRateNull = cntRateNull[1]*1.0/cntRateOverall[0]
        liftRateNull = cntRateNull[2]/cntRateOverall[1]
        popRateNonNull = 1.0 - popRateNull
        if popRateNonNull == 0.0:
            liftRateNonNull = -1.0
        else:
            liftRateNonNull = (1.0 - popRateNull*liftRateNull)/popRateNonNull

        if cntRateNull[2] <= cntRateOverall[1] * self.nullIssueLiftThreshold:
            return (False, popRateNull, liftRateNull, popRateNonNull, liftRateNonNull, -1, cntRateNull[2], cntRateOverall[1])

        # get the rate with respect to non-missing values
        cntRateNonNull = [[x, y[0], y[1]] for x, y in cntRateGrouped.items() if not self.isMissingAfterBucketing(x, colType)]

        # sort the buckets of non-missing values with respect to its rate
        cntRateNonNullSorted = sorted(cntRateNonNull, key = lambda x: x[2], reverse = True)

        # start from the bucket with highest rate to the bucket with lowest rate, find the cutoff value based on the threshhold
        pop = 0
        event = 0
        for cntRate in cntRateNonNullSorted:
            if pop <= cntRateOverall[0]*self.nullIssueToppopPercThreshold:
                pop += cntRate[1]
                event += cntRate[1]*cntRate[2]
            else:
                break


        # if (r_nm/r_0 - 1) < (r_m/r_0 - 1)*threshold where r_nm is the rte from non-mising values, r_m is from missing values, r_0 is overall rate, the feature has null issues.
        # return population rate of null values, lift from null values, population rate of non-null values and the lift from non-null values
        if (event/pop - cntRateOverall[1]) < self.nullIssuePredictiveThreshold*(cntRateNull[2] - cntRateOverall[1]):
            return (True, popRateNull, liftRateNull, popRateNonNull, liftRateNonNull, event/pop, cntRateNull[2], cntRateOverall[1])
        else:
            return (False, popRateNull, liftRateNull, popRateNonNull, liftRateNonNull, event/pop, cntRateNull[2], cntRateOverall[1])

    def getColumnsInfo(self):
        return {key: val for key, val in self.columnsInfo.items()}

    @overrides(ColumnRule)
    def getColumnsToRemove(self):
        return {key: val[0] for key, val in self.columnsInfo.items()}

    @overrides(ColumnRule)
    def getConfParameters(self):
        return { 'numBucket':self.numBucket,\
                 'nullIssueLiftThreshold':self.nullIssueLiftThreshold,\
                 'nullIssueToppopPercThreshold':self.nullIssueToppopPercThreshold,\
                 'nullIssuePredictiveThreshold':self.nullIssuePredictiveThreshold }

    @overrides(ColumnRule)
    def getResults(self):
        results = {}
        for col, testResult in self.columnsInfo.iteritems():
            rr = None
            if testResult is None:
                rr = RuleResults(True, 'Column not checked', {})
            else:
                (isFailed, popRateNull, liftRateNull, popRateNonNull, liftRateNonNull, convRate10, convRateNull, convRateOverall) = testResult
                if popRateNull == 0:
                    rr = RuleResults(True, 'NULL rate is 0%', {})
                elif convRate10 == -1:
                    rr = RuleResults(True,\
                            'NULL conv. rate {0:.2%} is less than {1}*{2:.2%}'.format(convRateNull, self.nullIssueLiftThreshold, convRateOverall),\
                            {'convRateNull':convRateNull, 'convRateOverall':convRateOverall})
                else:
                    rr = RuleResults(not isFailed,\
                            'NULL rate is {0:.2%}; top 10\% conv rate is {1:.2%}; overall conv. rate is {2:.2%}'.format(convRateNull, convRate10, convRateOverall),\
                            {'convRateNull':convRateNull, 'convRate10':convRate10, 'convRateOverall':convRateOverall})
            results[col] = rr
        return results

    def getSummaryPerColumn(self):
        return self.groupedCountAndConversionRate
