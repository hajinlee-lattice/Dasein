import logging

from leframework.codestyle import overrides
from rulefwk import ColumnRule, RuleResults


logger = logging.getLogger(name='futureinfo')

class FutureInfo(ColumnRule):

    def __init__(self, columns, maxNullPopulation, minNullLift, maxNonNullLift):
        super(FutureInfo, self).__init__(None)
        self.columns = columns
        self.maxNullPopulation = maxNullPopulation
        self.minNullLift = minNullLift
        self.maxNonNullLift = maxNonNullLift
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
                values = set()
                for record in profile[columnName]:
                    value = record['columnvalue']
                    if value not in values:
                        values.add(value)
                        totalRows += record['count']
                logger.info('Total Rows: {}'.format(totalRows))
                if totalRows == 0:
                    return

            nullLift = 1.0
            nullPopulation = 0.0
            nonNullEvents = 0
            nonNullRecords = 0
            for record in profile[columnName]:
                
                lift = record['lift']
                isNull = (record['discreteNullBucket'] or record['continuousNullBucket'])
                percentPopulation = float(record['count']) / float(totalRows)
                if isNull:
                    nullLift = lift
                    nullPopulation = percentPopulation
                else:
                    nonNullEvents += int(lift * record['count'])
                    nonNullRecords += record['count']

            nonNullLift = float(nonNullEvents) / float(nonNullRecords)

            if nullPopulation > self.maxNullPopulation and nullLift < self.minNullLift and nonNullLift > self.maxNonNullLift:
                ruleResult = RuleResults(False,
                        '{0:.2%} NULL pop., lift {1:.2} from NULL, lift {2:.2} from non-NULL'.format(nullPopulation, nullLift, nonNullLift),
                        {'nullPopulation':nullPopulation, 'nullLift':nullLift, 'nonNullLift':nonNullLift})
            else:
                ruleResult = RuleResults(True,
                        '{0:.2%} NULL pop., lift {1:.2} from NULL, lift {2:.2} from non-NULL'.format(nullPopulation, nullLift, nonNullLift),
                        {'nullPopulation':nullPopulation, 'nullLift':nullLift, 'nonNullLift':nonNullLift})

            self.results[columnName] = ruleResult

    @overrides
    def getDescription(self):
        return "This attribute looks like it was populated later in the business cycle, often called future information. This warning comes when available values show good lift (greater than {2}), but {0:.2%} or more records are unpopulated and have low lift (below {1}).".format(self.maxNullPopulation, self.minNullLift, self.maxNonNullLift)

    @overrides(ColumnRule)
    def getConfParameters(self):
        return {'maxNullPopulation':self.maxNullPopulation, 'minNullLift':self.minNullLift, 'maxNonNullLift':self.maxNonNullLift}

    @overrides(ColumnRule)
    def getResults(self):
        return self.results
