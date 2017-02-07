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
            for customerPredictor in self.customerPredictors:
                if customerPredictor in original_features:
                    logger.info("Removing customer predictor from features: {}".format(customerPredictor))
                    original_features.remove(customerPredictor)
            logger.info('Number of columns after rule remediation: %d' % len(dataFrame.columns))
        else:
            logger.info("No columns to remove")

        return dataFrame

    def includeInScoringPipeline(self):
        return False
