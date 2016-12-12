from rulefwk import ColumnRule, RuleResults
from leframework.codestyle import overrides

class DistinctValueCount(ColumnRule):

    def __init__(self, columns):
        super(DistinctValueCount, self).__init__(None)
        self.columns = columns

    @overrides(ColumnRule)
    def apply(self, dataFrame, columnMetadata, profile):
        print 'Applying a Rule to columns: {}'.format(str(self.columns))

    @overrides
    def getDescription(self):
        return "Check if categorical column has too many values from one value or numerical column has too many non-null values from one value"

    @overrides(ColumnRule)
    def getConfParameters(self):
        return {}

    @overrides(ColumnRule)
    def getResults(self):
        results = {}
        return results
