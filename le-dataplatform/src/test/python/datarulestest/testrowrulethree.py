from leframework.codestyle import overrides
from pipelinefwk import get_logger
from rulefwk import RowRule

logger = get_logger("TestRowRuleThree")

class TestRowRuleThree(RowRule):

    threshold = 2

    def __init__(self, threshold=3):
        self.threshold = threshold
        logger.info("Threshold value: %d" % threshold)

    @overrides(RowRule)
    def apply(self, dataFrame, configMetadata):
        return None

    @overrides(RowRule)
    def getRowsToRemove(self):
        return { "11869" : ['ColumnA', 'ColumnX', 'ColumnZ'],
                 "43441" : ['ColumnB'],
                 "Row3" : ['ColumnC'],
                 "Row4" : ['ColumnD']
               }

    @overrides(RowRule)
    def getDescription(self):
        return "A test row rule"