from rulefwk import ColumnRule, RuleResults
from leframework.codestyle import overrides
import logging

logger = logging.getLogger(name='valuepercentage')

class ValuePercentage(ColumnRule):

    def __init__(self, columns, maxPercentage):
        super(ValuePercentage, self).__init__(None)
        self.columns = columns
        self.maxPercentage = maxPercentage
        logger.info('Columns to check: {}'.format(str(self.columns)))
        self.results = {}

    @overrides(ColumnRule)
    def apply(self, dataFrame, columnMetadata, profile):

        totalRows = None

        for columnName in self.columns:
            ruleResult = None

            if columnName not in profile:
                continue

            if totalRows is None:
                totalRows = 0
                for record in profile[columnName]:
                    totalRows += record['count']
                logger.info('Total Rows: {}'.format(totalRows))
                if totalRows == 0:
                    return

            totalCount = 0
            includeColumn = True
            for record in profile[columnName]:

                hasMoreThan200DistinctValues = (record['Dtype'] == 'STR' and record['columnvalue'] in ['LATTICE_GT200_DistinctValue', 'LATTICE_GT200_DiscreteValue'])
                if hasMoreThan200DistinctValues:
                    includeColumn = False

                percentPopulation = float(record['count'])/float(totalRows)
                totalCount += record['count']
                
                if percentPopulation > self.maxPercentage:
                    ruleResult = RuleResults(False,
                            'Column has a single value with population rate {0:.2%} > {1:.2%}'.format(percentPopulation, self.maxPercentage),
                            {'percentPopulation':percentPopulation})

            if totalCount != totalRows and includeColumn:
                raise ValueError('Inconsistent row count: {0}, {1}, {2}'.format(columnName, totalCount,totalRows))

            if ruleResult is None or not includeColumn:
                ruleResult = RuleResults(True, 'Column has no value with population rate > {0:.2%}'.format(self.maxPercentage), {})

            self.results[columnName] = ruleResult

    @overrides
    def getDescription(self):
        return "This attribute has the same value for {0:.2%} or more records. This can lead to poor segments or inaccurate scores.".format(self.maxPercentage)

    @overrides(ColumnRule)
    def getConfParameters(self):
        return {'maxPercentage':self.maxPercentage}

    @overrides(ColumnRule)
    def getResults(self):
        return self.results
