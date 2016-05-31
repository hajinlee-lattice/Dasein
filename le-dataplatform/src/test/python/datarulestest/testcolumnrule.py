from leframework.codestyle import overrides
from pipelinefwk import get_logger
from rulefwk import ColumnRule

logger = get_logger("TestColumnRule")

class TestColumnRule(ColumnRule):

    threshold = 2

    def __init__(self, threshold=3):
        self.threshold = threshold
        logger.info("Threshold value: %d" % threshold)

    @overrides(ColumnRule)
    def apply(self, dataFrame, configMetadata):
        return None

    @overrides(ColumnRule)
    def getColumnsToRemove(self):
        return { "AColumn" : True,
                 "BColumn" : False,
                 "CColumn" : True
               }

    @overrides(ColumnRule)
    def getDescription(self):
        return "A test column rule"