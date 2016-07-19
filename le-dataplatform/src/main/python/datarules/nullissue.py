from rulefwk import ColumnRule, RuleResults
from leframework.codestyle import overrides
import numpy as np
from dataruleutils import getGroupedConversionRate, calculateOverallConversionRate, isNumerical, isCategorical

class NullIssue(ColumnRule):

    nullIssueToDelThreshold = 0.95
    nullIssueTopPopPercThreshold = 0.1
    nullIssueLiftThreshold = 1.5
    nullIssueColsToSkip = set(['Uses_Public_Email_Provider', 'Phone_Entropy', 'CompanyName_Length', 'SpamIndicator',
                                'Domain_Length', 'Title_Level', 'From_SFDC_LeadSource', 'FirstName_SameAs_LastName',
                                'Title_IsAcademic', 'Title_IsTechRelated', 'CompanyName_Entropy', 'Domain_IsClient', 'LeadSouce'])
    nullIssueNullValCat = ['', 'n/a', 'not available', 'empty', 'np.nan']
    columnsThatFailedTest = {}
    groupedCountAndConversionRate = {}

    def __init__(self, columns, categoricalColumns, numericalColumns, eventColumn, nullIssueToDelThreshold=0.95, nullIssueTopPopPercThreshold=0.1,
                  nullIssueLiftThreshold=1.5):
        self.columns = columns
        self.catColumn = categoricalColumns
        self.numColumn = numericalColumns
        self.eventColumn = eventColumn
        self.nullIssueToDelThreshold = nullIssueToDelThreshold
        self.nullIssueTopPopPercThreshold = nullIssueTopPopPercThreshold
        self.nullIssueLiftThreshold = nullIssueLiftThreshold

    @overrides(ColumnRule)
    def apply(self, dataFrame, dictOfArguments):
        self.results = {}
        for columnName, _ in self.columns.iteritems():
            if columnName in dataFrame:
                try:
                    columnType = self.getColumnType(columnName)
                    testResult = self.checkIfColumnHasNullIssues(dataFrame[columnName], dataFrame[self.eventColumn], columnType, columnName)
                    self.columnsThatFailedTest[columnName] = testResult
                except KeyError:
                    # What is default value
                    self.columnsThatFailedTest[columnName] = None

    def explain(self):
        return "given the conversion rate of each distinctive feature value and the overall conversion rate,    \
            decide if the null value imposed a problem to the model"

    def getColumnType(self, column):
        if column in self.catColumn:
            return "categorical"
        if column in self.numColumn:
            return "numerical"
        return None

    def checkIfColumnHasNullIssues(self, dataColumn, eventColumn, colType, columnName):

        overallPositiveCountAndConversionRate = calculateOverallConversionRate(eventColumn)

        dataColumn = dataColumn.replace(np.nan, "np.nan")

        groupedCountAndConversionRate = getGroupedConversionRate(dataColumn, eventColumn, overallPositiveCountAndConversionRate)
        self.groupedCountAndConversionRate.update({columnName: groupedCountAndConversionRate})

        cntRateNull = [[x, y[0], y[1], y[2]]  for x, y in groupedCountAndConversionRate.items()
                        if (isCategorical(colType) and str(x).lower() in self.nullIssueNullValCat) or (isNumerical(colType) and x == 'np.nan')]

        if isNumerical(colType) and x == 'np.nan':
            cntRateNull = [[x, y[0], y[1], y[2]]  for x, y in groupedCountAndConversionRate.items() if x == 'np.nan']

        if isCategorical(colType) and str(x).lower() in self.nullIssueNullValCat:
            cntRateNull = [[x, y[0], y[1], y[2]]  for x, y in groupedCountAndConversionRate.items() if str(x).lower() in self.nullIssueNullValCat]

        if len(cntRateNull) == 0:
            self.results[columnName] = RuleResults(True,'No NULL values to consider',{'cntRateNull':0})
            return False
        else:
            cntRateNull = cntRateNull[0]

        if int(cntRateNull[1]) >= self.nullIssueToDelThreshold * overallPositiveCountAndConversionRate[0]:
            percNull = float(cntRateNull[1])/overallPositiveCountAndConversionRate[0]
            rr = RuleResults(False,'Column is {0:.2%} NULL'.format(percNull),{'cntRateNull[1]':cntRateNull[1]})
            self.results[columnName] = RuleResults(False,'Column is {0:.2%} NULL'.format(percNull),{'cntRateNull[1]':cntRateNull[1]})
            return True

        cntRateNonNull = [[x, y[0], y[1], y[2]] for x, y in groupedCountAndConversionRate.items()
                           if (isCategorical(colType) and str(x).lower() not in self.nullIssueNullValCat) or (isNumerical(colType)and x != 0)]

        cntRateNonNullSorted = sorted(cntRateNonNull, key=lambda x: x[2], reverse=True)

        pop = 0
        event = 0
        for cntRate in cntRateNonNullSorted:
            if pop <= overallPositiveCountAndConversionRate[0] * self.nullIssueTopPopPercThreshold:
                pop += cntRate[1]
                event += cntRate[1] * cntRate[2]
            else:
                break

        if (event / pop - overallPositiveCountAndConversionRate[1]) < self.nullIssueLiftThreshold * (cntRateNull[2] - overallPositiveCountAndConversionRate[1]):
            self.results[columnName] = RuleResults(False,'NULL conv. rate {0:.2%} (Non-NULL rate {1:.2%}; Overall rate {2:.2%})'.format(cntRateNull[2], event/pop, overallPositiveCountAndConversionRate[1]),{'cntRateNull[2]':cntRateNull[2], 'event / pop':event/pop})
            return True
        else:
            self.results[columnName] = RuleResults(True,'NULL conv. rate {0:.2%} (Non-NULL rate {1:.2%}; Overall rate {2:.2%})'.format(cntRateNull[2], event/pop, overallPositiveCountAndConversionRate[1]),{'cntRateNull[2]':cntRateNull[2], 'event / pop':event/pop})
            return False

    @overrides(ColumnRule)
    def getConfParameters(self):
        return {
                'nullIssueToDelThreshold':self.nullIssueToDelThreshold,
                'nullIssueTopPopPercThreshold':self.nullIssueTopPopPercThreshold,
                'nullIssueLiftThreshold':self.nullIssueLiftThreshold }

    @overrides(ColumnRule)
    def getColumnsToRemove(self):
        return self.columnsThatFailedTest

    @overrides(ColumnRule)
    def getResults(self):
        return self.results

    def getSummaryPerColumn(self):
        return self.groupedCountAndConversionRate
