from leframework.codestyle import overrides
from rulefwk import RowRule

class AnonymousLeadRule(RowRule):

    def __init__(self):
        pass

    @overrides(RowRule)
    def apply(self, dataFrame, configMetadata):
        return None

    @overrides(RowRule)
    def getRowsToRemove(self):
        return None

    @overrides(RowRule)
    def getDescription(self):
        return "A lead is anonymous if it is missing all domain and company location fields"