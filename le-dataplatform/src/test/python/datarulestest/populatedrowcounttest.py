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
        self.assertEqual(len(populatedRowCountRule.getColumnsToRemove()), 0, "Rule should be False when all values are non-NaN values")

        dataFrame['A'][0:5] = np.NaN
        populatedRowCountRule.apply(dataFrame, dictOfArguments)
        self.assertEqual(populatedRowCountRule.getColumnsToRemove()['A'], True, "Rule should be True when there are 100% NaN values")

        dataFrame['A'][0] = np.NaN
        dataFrame['A'][1:] = "Non-Nan"
        populatedRowCountRule.apply(dataFrame, dictOfArguments)
        self.assertEqual(len(populatedRowCountRule.getColumnsToRemove()), 0, "Rule should be False when some values are non-NaN values and threshold is 0.0")

        dataFrame['A'][0] = np.NaN
        dataFrame['A'][1:] = "Non-Nan"
        populatedRowCountRule = PopulatedRowCount(columns, threshold=0.75)
        populatedRowCountRule.apply(dataFrame, dictOfArguments)
        self.assertEqual(populatedRowCountRule.getColumnsToRemove()['A'], True, "Rule should be True when there are more than 25% NaN values")

        dataFrame['A'][0] = "Non-Nan"
        dataFrame['A'][1:] = "Non-Nan"
        populatedRowCountRule = PopulatedRowCount(columns, threshold=0.25)
        populatedRowCountRule.apply(dataFrame, dictOfArguments)
        self.assertEqual(len(populatedRowCountRule.getColumnsToRemove()), 0, "Rule should be False when there are more than 25% non-NaN values")

        dataFrame['A'][0] = "Non-Nan"
        dataFrame['A'][1:] = np.NaN
        populatedRowCountRule = PopulatedRowCount(columns, threshold=0.25)
        populatedRowCountRule.apply(dataFrame, dictOfArguments)
        self.assertEqual(populatedRowCountRule.getColumnsToRemove()['A'], True, "Rule should be True when there are more than 25% NaN values")
