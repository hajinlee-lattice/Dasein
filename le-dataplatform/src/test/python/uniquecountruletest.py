from dataruletestbase import DataRuleTestBase
from datarules.countuniquevaluerule import CountUniqueValueRule
import numpy as np
from pandas.core.frame import DataFrame

class UniqueCountRuleTest(DataRuleTestBase):

    def testUniqueCountRule(self):
        dataFrame = DataFrame({u'A': range(1, 500), u'P1_Event': np.random.binomial(500, 0.2)})
        dictOfArguments = {}

        columns = {u"A":u"A" }
        catColumns = {u"A":u"A" }
        numColumn = {}
        eventColumn = u"P1_Event"

        uniqueCountRule = CountUniqueValueRule(columns=columns, categoricalColumns=catColumns, numericalColumns=numColumn, eventColumn=eventColumn)
        uniqueCountRule.apply(dataFrame, dictOfArguments)

        self.assertEqual(uniqueCountRule.getResults()['A'], True, "Rule should be True when there are more than 200 unique values")

        dataFrame = DataFrame({u'A': range(1, 150) , u'P1_Event': np.random.binomial(150, 0.2)})
        uniqueCountRule = CountUniqueValueRule(columns=columns, categoricalColumns=catColumns, numericalColumns=numColumn, eventColumn=eventColumn)
        uniqueCountRule.apply(dataFrame, dictOfArguments)

        self.assertEqual(uniqueCountRule.getResults()['A'], False, "Rule should be False when there are less than 200 unique values")

        dataFrame = DataFrame({u'A': range(1, 150) * 5 , u'P1_Event': np.random.binomial(150, 0.2)})
        uniqueCountRule = CountUniqueValueRule(columns=columns, categoricalColumns=catColumns, numericalColumns=numColumn, eventColumn=eventColumn)
        uniqueCountRule.apply(dataFrame, dictOfArguments)

        self.assertEqual(uniqueCountRule.getResults()['A'], False, "Rule should be False when there are less than 200 unique values")


