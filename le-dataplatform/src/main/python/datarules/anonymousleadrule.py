from leframework.codestyle import overrides
from rulefwk import RowRule

class AnonymousLeadRule(RowRule):

    def __init__(self):
        pass

    @overrides
    def apply(self, dataFrame, configMetadata):
        return None

    @overrides
    def getRowsToRemove(self):
        return None

    @overrides
    def getDescription(self):
        return "A lead is anonymous if it is missing all domain and company location fields"