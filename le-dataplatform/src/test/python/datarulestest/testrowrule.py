from leframework.codestyle import overrides
from pipelinefwk import get_logger
from rulefwk import RowRule

logger = get_logger("TestRowRule")

class TestRowRule(RowRule):

    threshold = 2

    def __init__(self, threshold=3):
        self.threshold = threshold
        logger.info("Threshold value: %d" % threshold)

    @overrides(RowRule)
    def apply(self, dataFrame, configMetadata):
        return None

    @overrides(RowRule)
    def getRowsToRemove(self):
        return { "13671" : ['ColumnA', 'ColumnX', 'ColumnZ'],
                 "28031" : ['ColumnB'],
                 "43441" : ['ColumnC'],
                 "44574" : ['ColumnD']
               }

    @overrides(RowRule)
    def getDescription(self):
        return "A test column rule"