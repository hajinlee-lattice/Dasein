'''
Description:

    This step will remove any categorical columns that have too many unique values
'''
import os
from pipelinefwk import PipelineStep
from pipelinefwk import get_logger

logger = get_logger("pipeline")

class HighNumberUniqueValuesRemovalStep(PipelineStep):

    ## The profiling algorithm ignores columns with
    ## a number of unique values greater than N; check
    ## the algorithm to ensure that maxNumberUniqueValues
    ## is no more than N.

    def __init__(self, maxNumberUniqueValues, categoricalColumns, params):
        self.maxNumberUniqueValues = maxNumberUniqueValues
        self.categoricalColumns = categoricalColumns
        self.params = params

    def transform(self, dataFrame, configMetadata, test):
        columnsToRemove = set()
        keys = self.params['schema']['keys']

        for colname in self.categoricalColumns:
            if colname in dataFrame.columns and colname not in keys:
                if len(dataFrame[colname].unique()) > self.maxNumberUniqueValues:
                    logger.info('Removing categorical column with more than {0} distinct values: "{1}".'.format(self.maxNumberUniqueValues, colname))
                    columnsToRemove.add(colname)

        self.removeColumns(dataFrame, columnsToRemove)
        return dataFrame

    def includeInScoringPipeline(self):
        return False
