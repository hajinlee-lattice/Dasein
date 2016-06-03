from dataruletestbase import DataRuleTestBase
from datarules.frequencyissue import FrequencyIssue
import pandas as pd

class FrequencyIssuesTest(DataRuleTestBase):

    def testFrequencyIssues(self):
        _, dataFrame = self.createDataFrameFromCSV()
        columns = {u"FirstName":u"FirstName", u"Email":u"Email" }
        catColumns = {u"FirstName":u"FirstName", u"Email":u"Email" }
        numericalColumn = {}
        eventColumn = u"P1_Event"
        frequencyIssueDetector = FrequencyIssue(
                                                                          columns,
                                                                          catColumns,
                                                                          numericalColumn,
                                                                          eventColumn)

        dictOfArguments = {}
        frequencyIssueDetector.apply(dataFrame, dictOfArguments)

        results = frequencyIssueDetector.getColumnsToRemove()

        self.assertTrue(results[u"FirstName"] == True, "FirstName should have True for FrequencyIssue")
        self.assertTrue(results[u"Email"] == True, "Domain should have True for Email")

        summary = frequencyIssueDetector.getSummaryPerColumn()
        self.checkCountConversionRatePValueForCatergoicalColumns(summary)
        self.checkCountConversionRatePValueForNumericalColumns(summary)

    def checkCountConversionRatePValueForNumericalColumns(self, summary):
        summaryName = summary[u"FirstName"]
        count, conversionRate, _ = summaryName['John']
        self.assertTrue(count == 1449, "Count for John should be 1449")
        self.assertAlmostEqual(conversionRate, 0.04899, msg="ConversionRate for John should be around 0.0489", delta=0.001)

        summaryEmail = summary[u"Email"]
        count, conversionRate, _ = summaryEmail['aol.com']
        self.assertTrue(count == 1094, "Count for aol.com should be 1094")
        self.assertAlmostEqual(conversionRate, 0.0, msg="ConversionRate for aol.com should be around 0.0", delta=0.001)

    def checkCountConversionRatePValueForCatergoicalColumns(self, summary):
        pass

    def createDataFrameFromCSV(self):
        fileName = "./data/DataRule/EventTable_MWB_NoCountryFilter_20160511.csv"
        dataFrame = pd.read_csv(fileName)
        colNames = dataFrame.columns

        return colNames, dataFrame
