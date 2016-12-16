from rulefwk import ColumnRule, RuleResults
from leframework.codestyle import overrides
import logging

logger = logging.getLogger(name='nulllift')

class NullLift(ColumnRule):

    def __init__(self, columns, minLift, maxLift):
        super(NullLift, self).__init__(None)
        self.columns = columns
        self.minLift = minLift
        self.maxLift = maxLift
        logger.info('Columns to check: {}'.format(str(self.columns)))
        self.results = {}

    @overrides(ColumnRule)
    def apply(self, dataFrame, columnMetadata, profile):

        for columnName in self.columns:
            ruleResult = None

            if columnName not in profile:
                continue

            for record in profile[columnName]:
                
                lift = record['lift']
                isNull = (record['discreteNullBucket'] or record['continuousNullBucket'])

                if isNull and (lift < self.minLift or lift > self.maxLift):
                    ruleResult = RuleResults(False,
                            'Column has lift {0:.2} from NULL values'.format(lift),
                            {'lift':lift})

            if ruleResult is None:
                ruleResult = RuleResults(True, 'Column has acceptable lift {0:.2} from NULL values'.format(lift), {'lift':lift})

            self.results[columnName] = ruleResult

    @overrides
    def getDescription(self):
        return "Attributes may not have lift from NULL values less than {0} or greater than {1}".format(self.minLift, self.maxLift)

    @overrides(ColumnRule)
    def getConfParameters(self):
        return {'minLift':self.minLift, 'maxLift':self.maxLift}

    @overrides(ColumnRule)
    def getResults(self):
        return self.results
