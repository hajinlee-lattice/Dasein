from dataruletestbase import DataRuleTestBase
from datarules.nullissue import NullIssue
import pandas as pd

class NullIssuesTest(DataRuleTestBase):

    def testNullIssues(self):
        _, dataFrame = self.createDataFrameFromCSV()
        columns = {u"FundingStage":u"FundingStage", u"FundingAmount":u"FundingAmount" }
        catColumns = { u"FundingStage":u"FundingStage" }
        numericalColumn = { u"FundingAmount":u"FundingAmount" }
        eventColumn = u"P1_Event"
        nullIssueDetector = NullIssue(
                                                                          columns,
                                                                          catColumns,
                                                                          numericalColumn,
                                                                          eventColumn)

        dictOfArguments = {}
        nullIssueDetector.apply(dataFrame, dictOfArguments)

        results = nullIssueDetector.getColumnsToRemove()

        self.assertTrue(results[u"FundingAmount"] == True, "FundingAmount(numerical column) should have True for NullIssues")
        self.assertTrue(results[u"FundingStage"] == True, "FundingStage(categorical column) should have True for NullIssues")

        summary = nullIssueDetector.getSummaryPerColumn()
        self.checkCountConversionRatePValueForCatergoicalColumns(summary)
        self.checkCountConversionRatePValueForNumericalColumns(summary)

    def checkCountConversionRatePValueForNumericalColumns(self, summary):
        summaryForFundingSAmount = summary[u"FundingAmount"]
        count, conversionRate, PValue = summaryForFundingSAmount['np.nan']
        self.assertTrue(count == 70666, "Count for 0 bucket should be 70666")
        self.assertAlmostEqual(conversionRate, 0.0577222, msg="ConversionRate for FundingAmount should be around 0.0577222", delta=0.001)
        self.assertAlmostEqual(PValue, -0.71672  , msg="PValue for FundingAmount should be -0.71672", delta=0.001)

    def checkCountConversionRatePValueForCatergoicalColumns(self, summary):
        summaryForModelAction = summary[u"FundingStage"]
        count, conversionRate, PValue = summaryForModelAction['np.nan']
        self.assertTrue(count == 71186, "CountForModelAction for FundingStage should be 71186")
        self.assertAlmostEqual(conversionRate, 0.058438456999971904, msg="ConverionsionRate for FundingStage should be around 0.058438456999971904", delta=0.001)
        self.assertAlmostEqual(PValue, -0.13999  , msg="PValue for FundingStage should be -0.13999", delta=0.001)

    def createDataFrameFromCSV(self):
        fileName = "./data/DataRule/EventTable_MWB_NoCountryFilter_20160511.csv"
        dataFrame = pd.read_csv(fileName)
        colNames = dataFrame.columns

        return colNames, dataFrame
