
# $LastChangedBy$
# $LastChangedDate$
# $Rev$

import logging
import pandas as pd

from dataruleeventtable import DataRuleEventTable
from datarules.populatedrowcountds import PopulatedRowCountDS
from datarules.lowcoverageds import LowCoverageDS
from datarules.nullissueds import NullIssueDS
from datarules.highlypredictivesmallpopulationds import HighlyPredictiveSmallPopulationDS
from datarules.uniquevaluecountds import UniqueValueCountDS
from datarules.overlypredictiveds import OverlyPredictiveDS
from testbase import TestBase


class AllDataRuleTests(TestBase):

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

    @classmethod
    def tearDownClass(cls):
        cls.logger.info("=========Tear down test: " + str(cls) + " ===========")

    ## arguments:
    ## * colrule: the DataRule object to test
    ## * dataframe: a pandas DataFrame, e.g., DataRuleEventTable.getDataFrame()
    ## * dictOfArgmuments: a dictionary of the arguments to pass to DataRule.apply()
    ## * reportset: a string specifying what results to print to STDOUT.  Valid options are
    ##   "failed", "passed", "all", "none"

    def columnRuleTestAlgorithm(self, colrule, dataframe, dictOfArguments, reportset='failed'):
        for p, v in colrule.getConfParameters().iteritems():
            self.logger.info('* Parameter {0:29s} = {1}'.format(p,v))
        colrule.apply(dataframe, dictOfArguments)
        n_cols = 0
        n_failed = 0
        for c, r in colrule.getResults().iteritems():
            n_cols += 1
            if not r.isPassed():
                n_failed += 1
            if not r.isPassed() and reportset in ['all', 'failed']:
                self.logger.info('! Failed Column {0}: {1}'.format(c,r.getMessage()))
            elif reportset in ['all', 'passed']:
                self.logger.info('o Passed Column {0}: {1}'.format(c,r.getMessage()))
        self.logger.info('* Summary: {0} columns failed ({1:.2%})'.format(n_failed, float(n_failed)/float(n_cols)))

    def rowRuleTestAlgorithm(self, rowrule, dataframe, dictOfArguments, reportset='failed'):
        for p, v in rowrule.getConfParameters().iteritems():
            self.logger.info('* Parameter {0:20s} = {1}'.format(p,v))
        rowrule.apply(dataframe, dictOfArguments)
        n_rows = len(dataframe.index)
        n_failed = 0
        for c, r in rowrule.getResults().iteritems():
            if not r.isPassed():
                n_failed += 1
            if not r.isPassed() and reportset in ['all', 'failed']:
                self.logger.info('! Failed Row {0}: {1}'.format(c,r.getMessage()))
            elif reportset in ['all', 'passed']:
                self.logger.info('o Passed Row {0}: {1}'.format(c,r.getMessage()))
        self.logger.info('* Summary: {0} rows failed ({1:.2%})'.format(n_failed, float(n_failed)/float(n_rows)))

    def testPopulatedRowCountDS(self):
        for et in [self.eventtable_nginx]:
            self.logger.info('________________________________________\n'+\
                    '                                                     '+\
                    'PopulatedRowCountDS: Using dataset {}'.format(et.getName()))
            columns = et.getAllColsAsDict()
            rule = PopulatedRowCountDS(columns, et.getCategoricalCols(), et.getNumericalCols(), 0.95, 0.95)
            dictOfArguments = {}
            self.columnRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')

    def IGNOREtestLowCoverageDS(self):
        for et in [self.eventtable_nginx]:
            self.logger.info('________________________________________\n'+\
                    '                                                     '+\
                    'LowCoverageDS: Using dataset {}'.format(et.getName()))
            columns = et.getAllColsAsDict()
            rule = LowCoverageDS(columns, et.getCategoricalCols(), et.getNumericalCols(), lowcoverageThreshold=0.95)
            dictOfArguments = {}
            self.columnRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')

    def testNullIssueDS(self):
        for et in [self.eventtable_nginx]:
            self.logger.info('________________________________________\n'+\
                    '                                                     '+\
                    'NullIssueDS: Using dataset {}'.format(et.getName()))
            columns = et.getAllColsAsDict()
            rule = NullIssueDS(columns, et.getCategoricalCols(), et.getNumericalCols(), et.getEventCol(),\
                    numBucket=20, nullIssueLiftThreshold=1.1, nullIssueToppopPercThreshold=0.1, nullIssuePredictiveThreshold=1.5)
            dictOfArguments = {}
            self.columnRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')

    def testUniqueValueCountDS(self):
        for et in [self.eventtable_nginx]:
            self.logger.info('________________________________________\n'+\
                    '                                                     '+\
                    'UniqueValueCountDS: Using dataset {}'.format(et.getName()))
            columns = et.getAllColsAsDict()
            rule = UniqueValueCountDS(columns, et.getCategoricalCols(), et.getNumericalCols(), uniquevaluecountThreshold=200)
            dictOfArguments = {}
            self.columnRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')

    def testOverlyPredictiveDS(self):
        for et in [self.eventtable_nginx]:
            self.logger.info('________________________________________\n'+\
                    '                                                     '+\
                    'OverlyPredictiveDS: Using dataset {}'.format(et.getName()))
            columns = et.getAllColsAsDict()
            rule = OverlyPredictiveDS(columns, et.getCategoricalCols(), et.getNumericalCols(), et.getEventCol(), 4, 0.05, 0.35)
            dictOfArguments = {}
            self.columnRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')

    def testHighlyPredictiveSmallPopulationDS(self):
        for et in [self.eventtable_nginx]:
            self.logger.info('________________________________________\n'+\
                    '                                                     '+\
                    'HighlyPredictiveSmallPopulationDS: Using dataset {}'.format(et.getName()))
            columns = et.getAllColsAsDict()
            rule = HighlyPredictiveSmallPopulationDS(columns, et.getCategoricalCols(), et.getNumericalCols(), et.getEventCol(), 3, 0.01)
            dictOfArguments = {}
            self.rowRuleTestAlgorithm(rule, et.getDataFrame(), dictOfArguments, 'none')
