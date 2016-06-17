from leframework.codestyle import overrides
from pipelinefwk import get_logger
from rulefwk import RowRule

logger = get_logger("AnonymousLeadRule")

class AnonymousLeadRule(RowRule):

    threshold = 2

    def __init__(self, threshold=3):
        self.threshold = threshold
        logger.info("Threshold value: %d" % threshold)

    @overrides(RowRule)
    def apply(self, dataFrame, configMetadata):
        return None

    @overrides(RowRule)
    def getRowsToRemove(self):
        return { "34ds" : ["ColumnA"] }

    @overrides(RowRule)
    def getDescription(self):
        return "A lead is anonymous if it is missing all domain and company location fields"