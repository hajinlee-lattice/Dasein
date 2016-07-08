from dataruletestbase import DataRuleTestBase
from datarules.leadperdomain import LeadPerDomainRule
import numpy as np
from pandas.core.frame import DataFrame

class leadPerDomainRuleTest(DataRuleTestBase):

    def testleadPerDomainRule(self):
        numberOfRows = 10

        dataFrame = DataFrame({
                               u'LeadID': range(1, numberOfRows),
                               u'CompanyName': range(1, numberOfRows),
                               u'Email': range(1, numberOfRows),
                               u'P1_Event': np.random.binomial(numberOfRows, 0.2)
                              })
        dictOfArguments = {}
        params = {}
        params["idColumn"] = "LeadID"

        leadPerDomainRule = LeadPerDomainRule(params)
        leadPerDomainRule.apply(dataFrame, dictOfArguments)
        rowsToRemove = leadPerDomainRule.getRowsToRemove()
        self.assertEqual(len(rowsToRemove.keys()), 0, "No keys should be filtered when Email and CompanyName are filled in")

        dataFrame['Email'][0:3] = 'test@test.com'
        dataFrame['Email'][3:6] = 'test1@test.com'
        dataFrame['Email'][6:] = 'test2@test.com'
        leadPerDomainRule.apply(dataFrame, dictOfArguments)
        rowsToRemove = leadPerDomainRule.getRowsToRemove()
        self.assertEqual(len(rowsToRemove.keys()), 6, "6 keys should be filtered when Email and CompanyName are filled in")

