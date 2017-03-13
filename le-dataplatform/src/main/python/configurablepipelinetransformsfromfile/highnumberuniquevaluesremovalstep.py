'''
Description:

    This step will remove any categorical columns that have too many unique values
'''
from pipelinefwk import PipelineStep
from pipelinefwk import get_logger

logger = get_logger("pipeline")

class HighNumberUniqueValuesRemovalStep(PipelineStep):

    # # The profiling algorithm ignores columns with
    # # a number of unique values greater than N

    def __init__(self, maxNumberUniqueValues, categoricalColumns, params, profile):
        # # maxNumberUniqueValues is retained for legacy pipeline drivers; it is not used anymore
        self.categoricalColumns = categoricalColumns
        self.params = params
        self.profile = profile

    def transform(self, dataFrame, configMetadata, test):
        columnsToRemove = set()
        keys = self.params['schema']['keys']

        for colname in self.categoricalColumns:
            if colname in dataFrame.columns and colname not in keys and colname in self.profile:
                for record in self.profile[colname]:
                    # # Categorical columns have Dtype == 'STR'
                    if record['Dtype'] != 'STR':
                        continue
                    # # The value LATTICE_GT200_DistinctValue is set when the column is profiled; see data_profile.py/data_profile_v2.py
                    if record['columnvalue'] in ['LATTICE_GT200_DistinctValue', 'LATTICE_GT200_DiscreteValue']:
                        logger.info('Removing categorical column with too many distinct values: "{0}".'.format(colname))
                        columnsToRemove.add(colname)
                        break

        self.removeColumns(dataFrame, columnsToRemove)
        return dataFrame

    def includeInScoringPipeline(self):
        return False
