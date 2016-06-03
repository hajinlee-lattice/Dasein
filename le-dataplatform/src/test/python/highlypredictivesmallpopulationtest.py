from dataruletestbase import DataRuleTestBase
from datarules.highlypredictivesmallpopulation import HighlyPredictiveSmallPopulation
import pandas as pd

class HighlyPredictiveSmallPopulationTest(DataRuleTestBase):

    def testHighlyPredictiveSmallPopulation(self):
        _, dataFrame = self.createDataFrameFromCSV()

        columns = {u"ModelAction":u"ModelAction", u"RetirementAssetsEOY":u"RetirementAssetsEOY" }
        catColumns = { u"ModelAction":u"ModelAction" }
        numericalColumn = { u"RetirementAssetsEOY":u"RetirementAssetsEOY" }
        eventColumn = u"P1_Event"
        highlyPredictiveSmallPopulation = HighlyPredictiveSmallPopulation(
                                                                          columns,
                                                                          catColumns,
                                                                          numericalColumn,
                                                                          eventColumn)

        dictOfArguments = {}
        highlyPredictiveSmallPopulation.apply(dataFrame, dictOfArguments)

        results = highlyPredictiveSmallPopulation.getColumnsToRemove()
        # Check that Model Action fails the rule
        self.assertTrue(results[u"ModelAction"] == True, "ModelAction(categorical column) should have True for HighlyPredictiveSmallPopulation")
        self.assertTrue(results[u"RetirementAssetsEOY"] == True, "RetirementAssetsEOY(numerical column) should have True for HighlyPredictiveSmallPopulation")

        summary = highlyPredictiveSmallPopulation.getSummaryPerColumn()
        self.checkCountConversionRatePValueForCatergoicalColumns(summary)
        self.checkCountConversionRatePValueForNumericalColumns(summary)

    def checkCountConversionRatePValueForNumericalColumns(self, summary):
        summaryForRetirementAssetsEOY = summary[u"RetirementAssetsEOY"]
        countForRetirementAssetsEOY, conversionRateForRetirementAssetsEOY, PValueForRetirementAssetsEOY = summaryForRetirementAssetsEOY[0]
        self.assertTrue(countForRetirementAssetsEOY == 230, "Count for 0 bucket should be 230")
        self.assertAlmostEqual(conversionRateForRetirementAssetsEOY, 0.10869, msg="ConverionsionRate for RetirementAssetsEOY should be around 0.10869", delta=0.001)
        self.assertAlmostEqual(PValueForRetirementAssetsEOY, 3.22417  , msg="PValue for RetirementAssetsEOY should be 3.22417", delta=0.001)

    def checkCountConversionRatePValueForCatergoicalColumns(self, summary):
        summaryForModelAction = summary[u"ModelAction"]
        countForModelAction, conversionRateForModelAction, PValueForModelAction = summaryForModelAction['RECENT BANKRUPTCY ON FILE']
        self.assertTrue(countForModelAction == 37, "CountForModelAction for Recent Bankruptcy should be 37")
        self.assertAlmostEqual(conversionRateForModelAction, 0.16216, msg="ConverionsionRate for Recent Bankruptcy should be around 0.16216", delta=0.001)
        self.assertAlmostEqual(PValueForModelAction, 2.6796  , msg="PValue for Recent Bankruptcy should be 2.6796", delta=0.001)

    def createDataFrameFromCSV(self):
        fileName = "./data/DataRule/EventTable_MWB_NoCountryFilter_20160511.csv"
        dataFrame = pd.read_csv(fileName)
        colNames = dataFrame.columns

        return colNames, dataFrame
