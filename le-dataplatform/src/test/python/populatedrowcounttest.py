from dataruletestbase import DataRuleTestBase
from datarules.populatedrowcount import PopulatedRowCount
import numpy as np
from pandas.core.frame import DataFrame

class PopulatedRowCountRuleTest(DataRuleTestBase):

    def testPopulatedRowCountRule(self):
        dataFrame = DataFrame({u'A': range(1, 5), u'P1_Event': np.random.binomial(5, 0.2)})
        dictOfArguments = {}

        columns = {u"A":u"A" }

        populatedRowCountRule = PopulatedRowCount(columns)
        populatedRowCountRule.apply(dataFrame, dictOfArguments)

        populatedRowCountRule.apply(dataFrame, dictOfArguments)
        self.assertEqual(populatedRowCountRule.getColumnsToRemove()['A'], False, "Rule should be False when there are more than 0.05% non-NaN values")

        dataFrame['A'][:499] = np.NaN
        populatedRowCountRule.apply(dataFrame, dictOfArguments)
        self.assertEqual(populatedRowCountRule.getColumnsToRemove()['A'], True, "Rule should be True when there are less than 0.05% non-NaN values")


