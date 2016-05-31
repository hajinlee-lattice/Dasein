from leframework.codestyle import overrides
from pipelinefwk import get_logger
from rulefwk import RowRule

logger = get_logger("TestRowRuleTwo")

class TestRowRuleTwo(RowRule):

    threshold = 2

    def __init__(self, threshold=3):
        self.threshold = threshold
        logger.info("Threshold value: %d" % threshold)

    @overrides(RowRule)
    def apply(self, dataFrame, configMetadata):
        return None

    @overrides(RowRule)
    def getRowsToRemove(self):
        return { "Row1" : True,
                 "Row2" : True,
                 "Row3" : False,
                 "Row4" : False,
               }

    @overrides(RowRule)
    def getDescription(self):
        return "A test column rule"