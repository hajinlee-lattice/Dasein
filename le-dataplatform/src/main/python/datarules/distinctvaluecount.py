from rulefwk import ColumnRule, RuleResults
from leframework.codestyle import overrides
import logging

logger = logging.getLogger(name='distinctvaluecount')

class DistinctValueCount(ColumnRule):

    def __init__(self, columns):
        super(DistinctValueCount, self).__init__(None)
        self.columns = columns
        logger.info('Columns to check: {}'.format(str(self.columns)))
        self.results = {}

    @overrides(ColumnRule)
    def apply(self, dataFrame, columnMetadata, profile):

        for columnName in self.columns:
            ruleResult = None

            if columnName not in profile:
                continue

            for record in profile[columnName]:
                ## Categorical columns have Dtype == 'STR'
                if record['Dtype'] != 'STR':
                    continue
                ## The value LATTICE_GT200_DistinctValue is set when the column is profiled; see data_profile.py/data_profile_v2.py
                if record['columnvalue'] in ['LATTICE_GT200_DistinctValue', 'LATTICE_GT200_DiscreteValue']:
                    ruleResult = RuleResults(False, 'Column has more than 200 distinct values', {})
                    break

            if ruleResult is None:
                ruleResult = RuleResults(True, "Column has no more than 200 distinct values", {})

            self.results[columnName] = ruleResult

    @overrides
    def getDescription(self):
        return "Categorical attributes may not have more than 200 distinct values"

    @overrides(ColumnRule)
    def getConfParameters(self):
        return {}

    @overrides(ColumnRule)
    def getResults(self):
        return self.results
