from pipelinefwk import PipelineStep
from pipelinefwk import get_logger

logger = get_logger("remediatedatarules")

class RemediateDataRulesStep(PipelineStep):

    def __init__(self, params, enabledRules={}, customerPredictors=[]):
        self.enabledRules = enabledRules
        self.customerPredictors = customerPredictors
        self.params = params
        self.columnRules = {}
        self.rowRules = {}

    def transform(self, dataFrame, columnMetadata, test):
        columnsToRemove = set()

        for ruleName, items in self.enabledRules.items():
            items = [i for i in items if i in dataFrame.columns.values]
            columnsToRemove.update(items)
            logger.info('Items to remove for rule %s: %s' % (ruleName, str(items)))

        original_features = []
        if "original_features" in self.params["schema"] and self.params["schema"]["original_features"] != None:
            original_features = self.params["schema"]["original_features"]
        logger.info('Original Features: {}'.format(str(original_features)))

        if columnsToRemove:
            logger.info("Removing columns %s" % columnsToRemove)
            logger.info('Number of columns before rule remediation: %d' % len(dataFrame.columns))
            super(RemediateDataRulesStep, self).removeColumns(dataFrame, columnsToRemove)
            ## Note: If customerPredictors are not removed from original_features, then a customer column
            ## may have ApprovedUsage=None and not be in the actual model, but the column will be included
            ## in 'InputColumnMetadata' in model.json.  This will mean (1) the column is not requested
            ## from the user when bulk scoring, but (2) validation will fail because the column is not present.
            ## See PLS-2965.
            logger.info('Number of columns after rule remediation: %d' % len(dataFrame.columns))
        else:
            logger.info("No columns to remove")

        return dataFrame

    def includeInScoringPipeline(self):
        return False
