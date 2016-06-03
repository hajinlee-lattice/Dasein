from dataruletestbase import DataRuleTestBase
from datarules.futureinformation import FutureInformation
import pandas as pd

class FutureInformationTest(DataRuleTestBase):

    def testFutureInformation(self):
        _, dataFrame = self.createDataFrameFromCSV()
        columns = {u"CompanyName_Entropy":u"CompanyName_Entropy", u"CompanyName_Length":u"CompanyName_Length" }
        catColumns = { }
        numericalColumn = { u"CompanyName_Entropy":u"CompanyName_Entropy", u"CompanyName_Length":u"CompanyName_Length" }
        eventColumn = u"P1_Event"
        futureInformationIssueDetector = FutureInformation(
                                                                          columns,
                                                                          catColumns,
                                                                          numericalColumn,
                                                                          eventColumn)

        dictOfArguments = {}
        futureInformationIssueDetector.apply(dataFrame, dictOfArguments)

        results = futureInformationIssueDetector.getColumnsToRemove()

        self.assertTrue(results[u"CompanyName_Entropy"] == True, "CompanyName_Entropy should have True for Future Information")
        self.assertTrue(results[u"CompanyName_Length"] == True, "CompanyName_Length should have True for Future Information")

        summary = futureInformationIssueDetector.getSummaryPerColumn()
        self.checkCountConversionRatePValueForCatergoicalColumns(summary)
        self.checkCountConversionRatePValueForNumericalColumns(summary)

    def checkCountConversionRatePValueForNumericalColumns(self, summary):
        pass

    def checkCountConversionRatePValueForCatergoicalColumns(self, summary):
        summaryForModelAction = summary[u"CompanyName_Entropy"]
        count, conversionRate, PValue = summaryForModelAction['np.nan']
        self.assertTrue(count == 660, "Count should be 660")
        self.assertAlmostEqual(conversionRate, 0.278788, msg="ConverionsionRate should be around 0.278788", delta=0.001)
        self.assertAlmostEqual(PValue, 23.5928  , msg="PValue should be 23.5928", delta=0.001)

        summaryForModelAction = summary[u"CompanyName_Length"]
        count, conversionRate, PValue = summaryForModelAction[1]
        self.assertTrue(count == 680, "Count should be 680")
        self.assertAlmostEqual(conversionRate, 0.270588, msg="ConverionsionRate should be around 0.278788", delta=0.001)
        self.assertAlmostEqual(PValue, 23.05555  , msg="PValue should be 23.05555", delta=0.001)

    def createDataFrameFromCSV(self):
        fileName = "./data/DataRule/EventTable_MWB_NoCountryFilter_20160511.csv"
        dataFrame = pd.read_csv(fileName)
        colNames = dataFrame.columns

        return colNames, dataFrame
