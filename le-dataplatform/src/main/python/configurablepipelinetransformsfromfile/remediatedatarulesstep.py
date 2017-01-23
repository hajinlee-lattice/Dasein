from pipelinefwk import PipelineStep
from pipelinefwk import get_logger

logger = get_logger("remediatedatarules")

class RemediateDataRulesStep(PipelineStep):

    def __init__(self, params, enabledRules={}):
        self.enabledRules = enabledRules
        self.params = params
        self.columnRules = {}
        self.rowRules = {}

    def transform(self, dataFrame, columnMetadata, test):
        columnsToRemove = set()

        for ruleName, items in self.enabledRules.items():
            items = [i for i in items if i in dataFrame.columns.values]
            columnsToRemove.update(items)
            logger.info('Items to remove for rule %s: %s' % (ruleName, str(items)))

        if columnsToRemove:
            logger.info("Removing columns %s" % columnsToRemove)
            logger.info('Number of columns before rule remediation: %d' % len(dataFrame.columns))
            super(RemediateDataRulesStep, self).removeColumns(dataFrame, columnsToRemove)
            logger.info('Number of columns after rule remediation: %d' % len(dataFrame.columns))
        else:
            logger.info("No columns to remove")

        return dataFrame

    def includeInScoringPipeline(self):
        return False
