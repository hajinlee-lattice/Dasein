from dataruletestbase import DataRuleTestBase
from datarules.anonymousleadrule import AnonymousLeadRule
import numpy as np
from pandas.core.frame import DataFrame

class anonymousLeadRuleTest(DataRuleTestBase):

    def testanonymousLeadRule(self):
        dataFrame = DataFrame({
                               u'LeadID': range(1, 5),
                               u'CompanyName': range(1, 5),
                               u'Email': range(1, 5),
                               u'P1_Event': np.random.binomial(5, 0.2)
                              })
        dictOfArguments = {}
        params = {}
        params["idColumn"] = "LeadID"
        anonymousLeadRule = AnonymousLeadRule(params=params)
        anonymousLeadRule.apply(dataFrame, dictOfArguments)
        rowsToRemove = anonymousLeadRule.getRowsToRemove()
        self.assertEqual(len(rowsToRemove.keys()), 0, "No keys should be filtered when Email and CompanyName are filled in")

        anonymousLeadRule.apply(dataFrame, dictOfArguments)
        rowsToRemove = anonymousLeadRule.getRowsToRemove()
        self.assertEqual(len(rowsToRemove.keys()), 0, "No keys should be filtered when Email and CompanyName are filled in")

        dataFrame['Email'][1:2] = None
        dataFrame['CompanyName'][1:2] = None
        anonymousLeadRule.apply(dataFrame, dictOfArguments)
        rowsToRemove = anonymousLeadRule.getRowsToRemove()
        self.assertEqual(len(rowsToRemove.keys()), 1, "One key should be filtered when Email is empty")
        self.assertEqual(rowsToRemove.keys(), [2], "Row with LeadID 2 should be filtered when Email is empty")

        dataFrame['Email'] = self.resetDataFrame()
        anonymousLeadRule.apply(dataFrame, dictOfArguments)
        rowsToRemove = anonymousLeadRule.getRowsToRemove()
        self.assertEqual(len(rowsToRemove.keys()), 0, "No keys should be filtered when Email and CompanyName are filled in")

        dataFrame['Email'][2:5] = None
        dataFrame['CompanyName'][2:5] = None
        anonymousLeadRule.apply(dataFrame, dictOfArguments)
        rowsToRemove = anonymousLeadRule.getRowsToRemove()
        self.assertEqual(len(rowsToRemove.keys()), 2, "Two keys should be filtered when CompanyName is empty")
        self.assertEqual(rowsToRemove.keys(), [3, 4], "Row with LeadID 3 and 4 should be filtered when CompanyName is empty")

        print rowsToRemove

    def resetDataFrame(self):
        return range(1, 5)
