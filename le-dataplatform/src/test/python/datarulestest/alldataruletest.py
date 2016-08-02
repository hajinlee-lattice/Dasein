
# $LastChangedBy$
# $LastChangedDate$
# $Rev$

import logging

from dataruleeventtable import DataRuleEventTable
from datarules.populatedrowcountds import PopulatedRowCountDS
from datarules.lowcoverageds import LowCoverageDS
from datarules.nullissueds import NullIssueDS
from datarules.highlypredictivesmallpopulationds import HighlyPredictiveSmallPopulationDS
from datarules.uniquevaluecountds import UniqueValueCountDS
from datarules.overlypredictiveds import OverlyPredictiveDS
from testbase import TestBase

class AllDataRuleTest(TestBase):

    eventtable_mulesoft = None
    logger = logging.getLogger(name='AllDataRuleTests')


    @classmethod
    def setUpClass(cls):
        cls.logger.info("=========Current test: " + str(cls) + " ===========")
        cls.eventtable_mulesoft = DataRuleEventTable(
                'Mulesoft NA',
                'Mulesoft_Migration_LP3_ModelingLead_ReducedRows_Training_20160624_155355.avro')
        cls.eventtable_telogis = DataRuleEventTable(
                'Telogis POC',
                'Telogis_POC_Training.avro')
        cls.eventtable_hostingcom = DataRuleEventTable(
                'Hosting.com POC',
                'Hostingcom_POC_Training.avro')
        cls.eventtable_alfresco = DataRuleEventTable(
                'Alfresco',
                'Alfresco_SFDC_LP3_ModelingLead_ReducedRowsEnhanced_Training_20160712_125241.avro')
        cls.eventtable_nginx = DataRuleEventTable(
                'NGINX',
                'NGINX_PLS_LP3_ModelingLead_ReducedRowsEnhanced_Training_20160712_125224.avro')
        cls.eventtable_seagate = DataRuleEventTable(
                'Seagate',
                'Seagate.avro')

    @classmethod
    def tearDownClass(cls):
        cls.logger.info("=========Tear down test: " + str(cls) + " ===========")

    # # arguments:
    # # * colrule: the DataRule object to test
    # # * dataframe: a pandas DataFrame, e.g., DataRuleEventTable.getDataFrame()
    # # * dictOfArgmuments: a dictionary of the arguments to pass to DataRule.apply()
    # # * reportset: a string specifying what results to print to STDOUT.  Valid options are
    # #   "failed", "passed", "all", "none"

    def columnRuleTestAlgorithm(self, colrule, dataframe, dictOfArguments, reportset='failed'):
        colstoremove = set()
        for p, v in colrule.getConfParameters().iteritems():
            self.logger.info('* Parameter {0:29s} = {1}'.format(p, v))
        colrule.apply(dataframe, dictOfArguments)
        n_cols = 0
        n_failed = 0
        details = ''
        detailedresults = colrule.getResults()
        for c, r in colrule.getResults().iteritems():
            n_cols += 1
            if not r.isPassed():
                colstoremove.add(c)
                n_failed += 1
            if not r.isPassed() and reportset in ['all', 'failed']:
                self.logger.info('! Failed Column {0}: {1}'.format(c, r.getMessage()))
            elif reportset in ['all', 'passed']:
                self.logger.info('o Passed Column {0}: {1}'.format(c, r.getMessage()))
        self.logger.info('* Summary: {0} columns failed ({1:.2%})'.format(n_failed, float(n_failed) / float(n_cols)))
        return colstoremove

    def columnRuleTestAlgorithmMany(self, colrulelist, dataframe, reportset='failed'):
        colstoremove = set()
        for colrule in colrulelist:
            self.logger.info('________________________________________\n' + \
                        '                                                     ' + \
                        '{}'.format(type(colrule).__name__))
            colstoremove = colstoremove | self.columnRuleTestAlgorithm(colrule, dataframe, {}, reportset)
        colstr = ''
        for c in sorted(colstoremove):
            colstr += (c + '\n')
        self.logger.info('* ALL COLUMNS TO REMOVE ({0} TOTAL):\n{1}'.format(len(colstoremove), colstr))

    def rowRuleTestAlgorithm(self, rowrule, dataframe, dictOfArguments, reportset='failed'):
        for p, v in rowrule.getConfParameters().iteritems():
            self.logger.info('* Parameter {0:20s} = {1}'.format(p, v))
        rowrule.apply(dataframe, dictOfArguments)
        n_rows = len(dataframe.index)
        n_failed = 0
        for c, r in rowrule.getResults().iteritems():
            if not r.isPassed():
                n_failed += 1
            if not r.isPassed() and reportset in ['all', 'failed']:
                self.logger.info('! Failed Row {0}: {1}'.format(c, r.getMessage()))
            elif reportset in ['all', 'passed']:
                self.logger.info('o Passed Row {0}: {1}'.format(c, r.getMessage()))
        self.logger.info('* Summary: {0} rows failed ({1:.2%})'.format(n_failed, float(n_failed) / float(n_rows)))

    def atestPopulatedRowCountDS(self, doParameterSearch=False):
        for et in [self.eventtable_hostingcom]:
            if doParameterSearch:
                self.logger.info('________________________________________\n' + \
                    '                                                     ' + \
                    'Parameter Search PopulatedRowCountDS: Using dataset {}'.format(et.getName()))
                columns = et.getAllColsAsDict()
                catThresholdRange = [0.98, 0.99, 0.995]
                numThresholdRange = catThresholdRange
                for x in catThresholdRange:
                    self.logger.info("Searching PopulatedRowCount catThresholdRange")
                    rule = PopulatedRowCountDS(columns, et.getCategoricalCols(), et.getNumericalCols(),
                                               populatedrowcountCatThreshold=x,
                                               populatedrowcountNumThreshold=0.98)
                    dictOfArguments = {}
                    self.rowRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')
                for y in numThresholdRange:
                    self.logger.info("Searching PopulatedRowCount numThresholdRange")
                    rule = PopulatedRowCountDS(columns, et.getCategoricalCols(), et.getNumericalCols(),
                                               populatedrowcountCatThreshold=0.98,
                                               populatedrowcountNumThreshold=y)
                    dictOfArguments = {}
                    self.rowRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')
            else:
                self.logger.info('________________________________________\n' + \
                        '                                                     ' + \
                        'PopulatedRowCountDS: Using dataset {}'.format(et.getName()))
                columns = et.getAllColsAsDict()
                rule = PopulatedRowCountDS(columns, et.getCategoricalCols(), et.getNumericalCols(), 0.95, 0.95)
                dictOfArguments = {}
                self.columnRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')

    def atestLowCoverageDS(self, doParameterSearch=False):
        for et in [self.eventtable_hostingcom]:
            if doParameterSearch:
                self.logger.info('________________________________________\n' + \
                        '                                                     ' + \
                        'Parameter Search LowCoverageDS: Using dataset {}'.format(et.getName()))
                columns = et.getAllColsAsDict()
                lowCoverageThresholdRange = [0.9, 0.95, 1.0]
                for x in lowCoverageThresholdRange:
                    rule = LowCoverageDS(columns, et.getCategoricalCols(), et.getNumericalCols(), lowcoverageThreshold=x)
                    dictOfArguments = {}
                    self.columnRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')
            else:
                self.logger.info('________________________________________\n' + \
                        '                                                     ' + \
                        'LowCoverageDS: Using dataset {}'.format(et.getName()))
                columns = et.getAllColsAsDict()
                rule = LowCoverageDS(columns, et.getCategoricalCols(), et.getNumericalCols(), lowcoverageThreshold=0.95)
                dictOfArguments = {}
                self.columnRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')

    def atestNullIssueDS(self, doParameterSearch=False):
        for et in [self.eventtable_hostingcom]:
            if doParameterSearch:
                self.logger.info('________________________________________\n' + \
                        '                                                     ' + \
                        'Parameter Search NullIssueDS: Using dataset {}'.format(et.getName()))
                columns = et.getAllColsAsDict()
                numBucketRange = [10, 20, 30]
                nullIssueLiftThresholdRange = [1.0, 1.1, 1.2, 1.3]
                nullIssueToppopPercThresholdRange = [0.1, 0.05, 0.15]
                nullIssuePredictiveThresholdRange = [0.8, 1.0, 1.3, 1.5, 2.0]
                for a in numBucketRange:
                    self.logger.info("Searching NullIssue buckets")
                    rule = NullIssueDS(columns, et.getCategoricalCols(), et.getNumericalCols(), et.getEventCol(), \
                        numBucket=a, nullIssueLiftThreshold=1.1, nullIssueToppopPercThreshold=0.1, nullIssuePredictiveThreshold=1.5)
                    dictOfArguments = {}
                    self.columnRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')
                for a in nullIssueLiftThresholdRange:
                    self.logger.info("Searching NullIssue liftThreshold")
                    rule = NullIssueDS(columns, et.getCategoricalCols(), et.getNumericalCols(), et.getEventCol(), \
                        numBucket=20, nullIssueLiftThreshold=a, nullIssueToppopPercThreshold=0.1, nullIssuePredictiveThreshold=1.5)
                    dictOfArguments = {}
                    self.columnRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')
                for a in nullIssueToppopPercThresholdRange:
                    self.logger.info("Searching NullIssue nullIssueToppopPercThresholdRange")
                    rule = NullIssueDS(columns, et.getCategoricalCols(), et.getNumericalCols(), et.getEventCol(), \
                        numBucket=20, nullIssueLiftThreshold=1.1, nullIssueToppopPercThreshold=a, nullIssuePredictiveThreshold=1.5)
                    dictOfArguments = {}
                    self.columnRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')
                for a in nullIssuePredictiveThresholdRange:
                    self.logger.info("Searching NullIssue nullIssuePredictiveThresholdRange")
                    rule = NullIssueDS(columns, et.getCategoricalCols(), et.getNumericalCols(), et.getEventCol(), \
                        numBucket=20, nullIssueLiftThreshold=1.1, nullIssueToppopPercThreshold=0.1, nullIssuePredictiveThreshold=a)
                    dictOfArguments = {}
                    self.columnRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')
            else:
                self.logger.info('________________________________________\n' + \
                    '                                                     ' + \
                    'NullIssueDS: Using dataset {}'.format(et.getName()))
                columns = et.getAllColsAsDict()
                rule = NullIssueDS(columns, et.getCategoricalCols(), et.getNumericalCols(), et.getEventCol(), \
                        numBucket=20, nullIssueLiftThreshold=1.1, nullIssueToppopPercThreshold=0.1, nullIssuePredictiveThreshold=1.5)
                dictOfArguments = {}
                self.columnRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')

    def atestUniqueValueCountDS(self, doParameterSearch=False):
        for et in [self.eventtable_hostingcom]:
            if doParameterSearch:
                self.logger.info('________________________________________\n' + \
                        '                                                     ' + \
                        'Parameter Search UniqueValueCountDS: Using dataset {}'.format(et.getName()))
                columns = et.getAllColsAsDict()
                uniqueCountThresholdRange = [100, 200, 400]
                for x in uniqueCountThresholdRange:
                    rule = UniqueValueCountDS(columns, et.getCategoricalCols(), et.getNumericalCols(), uniquevaluecountThreshold=x)
                    dictOfArguments = {}
                    self.columnRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')
            else:
                self.logger.info('________________________________________\n' + \
                    '                                                     ' + \
                    'UniqueValueCountDS: Using dataset {}'.format(et.getName()))
                columns = et.getAllColsAsDict()
                rule = UniqueValueCountDS(columns, et.getCategoricalCols(), et.getNumericalCols(), uniquevaluecountThreshold=200)
                dictOfArguments = {}
                self.columnRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')

    def atestOverlyPredictiveDS(self, doParameterSearch=False):
        for et in [self.eventtable_hostingcom]:
            if doParameterSearch:
                self.logger.info('________________________________________\n' + \
                        '                                                     ' + \
                        'Parameter Search OverlyPredictiveDS: Using dataset {}'.format(et.getName()))
                columns = et.getAllColsAsDict()
                overlypredictiveLiftThresholdRange = [4, 6, 8, 10]
                overlypredictivePopThresholdRange = [0.025, 0.05, 0.1, 0.2, 0.4]
                scaleRange = [0.1, 0.35, 0.7, 1.0]
                for x in overlypredictiveLiftThresholdRange:
                    rule = OverlyPredictiveDS(columns, et.getCategoricalCols(), et.getNumericalCols(), et.getEventCol(),
                                              overlypredictiveLiftThreshold=x,
                                              overlypredictivePopThreshold=0.05,
                                              scale=0.35)
                    dictOfArguments = {}
                    self.columnRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')
                for x in overlypredictivePopThresholdRange:
                    rule = OverlyPredictiveDS(columns, et.getCategoricalCols(), et.getNumericalCols(), et.getEventCol(),
                                              overlypredictiveLiftThreshold=4,
                                              overlypredictivePopThreshold=x,
                                              scale=0.35)
                    dictOfArguments = {}
                    self.columnRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')
                for x in scaleRange:
                    rule = OverlyPredictiveDS(columns, et.getCategoricalCols(), et.getNumericalCols(), et.getEventCol(),
                                              overlypredictiveLiftThreshold=4,
                                              overlypredictivePopThreshold=0.05,
                                              scale=x)
                    dictOfArguments = {}
                    self.columnRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')
            else:
                self.logger.info('________________________________________\n' + \
                    '                                                     ' + \
                    'OverlyPredictiveDS: Using dataset {}'.format(et.getName()))
                columns = et.getAllColsAsDict()
                rule = OverlyPredictiveDS(columns, et.getCategoricalCols(), et.getNumericalCols(), et.getEventCol(), 4, 0.05, 0.35)
                dictOfArguments = {}
                self.columnRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')

    def atestHighlyPredictiveSmallPopulationDS(self, doParameterSearch=False):
        for et in [self.eventtable_hostingcom]:
            if doParameterSearch:
                self.logger.info('________________________________________\n' + \
                        '                                                     ' + \
                        'Parameter Search HighlyPredictiveSmallPopulationDS: Using dataset {}'.format(et.getName()))
                columns = et.getAllColsAsDict()
                highlyPredictiveSmallPopulationLiftThresholdRange = [3, 5, 10, 15]
                highlyPredictiveSmallPopulationPopThresholdRange = [0.005, 0.01, 0.001]
                for x in highlyPredictiveSmallPopulationLiftThresholdRange:
                    rule = HighlyPredictiveSmallPopulationDS(columns, et.getCategoricalCols(), et.getNumericalCols(), et.getEventCol(),
                                                             highlyPredictiveSmallPopulationLiftThreshold=x,
                                                             highlyPredictiveSmallPopulationPopThreshold=0.005)
                    dictOfArguments = {}
                    self.rowRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')
                for x in highlyPredictiveSmallPopulationPopThresholdRange:
                    rule = HighlyPredictiveSmallPopulationDS(columns, et.getCategoricalCols(), et.getNumericalCols(), et.getEventCol(),
                                                             highlyPredictiveSmallPopulationLiftThreshold=4,
                                                             highlyPredictiveSmallPopulationPopThreshold=x)
                    dictOfArguments = {}
                    self.rowRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')
            else:
                self.logger.info('________________________________________\n' + \
                    '                                                     ' + \
                    'HighlyPredictiveSmallPopulationDS: Using dataset {}'.format(et.getName()))
                columns = et.getAllColsAsDict()
                rule = HighlyPredictiveSmallPopulationDS(columns, et.getCategoricalCols(), et.getNumericalCols(), et.getEventCol(), 3, 0.01)
                dictOfArguments = {}
                self.rowRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')

    def atestAll(self):
        for et in [self.eventtable_hostingcom]:
            self.logger.info('________________________________________\n' + \
                    '                                                     ' + \
                    'DataRules: Using dataset {}'.format(et.getName()))
            self.atestHighlyPredictiveSmallPopulationDS()
            self.atestNullIssueDS()
            self.atestPopulatedRowCountDS()
            self.atestOverlyPredictiveDS()
            self.atestUniqueValueCountDS()

    def atestdoParameterSearch(self):
        for et in [self.eventtable_hostingcom]:
            self.logger.info('________________________________________\n' + \
                    '                                                     ' + \
                    'Parameter Search DataRules: Using dataset {}'.format(et.getName()))
            self.atestHighlyPredictiveSmallPopulationDS(doParameterSearch=True)
            self.atestNullIssueDS(doParameterSearch=True)
            self.atestPopulatedRowCountDS(doParameterSearch=True)
            self.atestOverlyPredictiveDS(doParameterSearch=True)
            self.atestUniqueValueCountDS(doParameterSearch=True)

    def testCombination(self):
        for et in [self.eventtable_hostingcom]:
            colrulelist = []
            colrulelist.append(PopulatedRowCountDS(et.getAllColsAsDict(), et.getCategoricalCols(), et.getNumericalCols()))
            colrulelist.append(LowCoverageDS(et.getAllColsAsDict(), et.getCategoricalCols(), et.getNumericalCols()))
            colrulelist.append(NullIssueDS(et.getAllColsAsDict(), et.getCategoricalCols(), et.getNumericalCols(), et.getEventCol()))
            colrulelist.append(UniqueValueCountDS(et.getAllColsAsDict(), et.getCategoricalCols(), et.getNumericalCols()))
            colrulelist.append(OverlyPredictiveDS(et.getAllColsAsDict(), et.getCategoricalCols(), et.getNumericalCols(), et.getEventCol()))
            self.columnRuleTestAlgorithmMany(colrulelist, et.getDataFrame(), 'none')
