
# $LastChangedBy$
# $LastChangedDate$
# $Rev$

import logging

from dataruleeventtable import DataRuleEventTable
from datarules.distinctvaluecount import DistinctValueCount
from testbase import TestBase

class AllDataRuleTest(TestBase):

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

    def columnRuleTestAlgorithm(self, colrule, dataframe, columnMetadata, profile, reportset='failed'):
        colstoremove = set()
        for p, v in colrule.getConfParameters().iteritems():
            self.logger.info('* Parameter {0:29s} = {1}'.format(p, v))
        colrule.apply(dataframe, columnMetadata, profile)
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
        self.logger.info('* Summary: {0} columns failed ({1:.2%})'.format(n_failed, float(n_failed) / float(n_cols) if n_cols > 0 else 0.0))
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

    def testDistinctValueCount(self):
        for et in [self.eventtable_nginx]:
            self.logger.info('________________________________________\n' + \
                        '                                                     ' + \
                        'DistinctValueCount: Using dataset {}'.format(et.getName()))
            columns = et.getAllColsAsDict()
            rule = DistinctValueCount(columns)
            columnMetadata = {}
            self.columnRuleTestAlgorithm(rule, et.getDataFrame(), columnMetadata, None, 'all')

    def _testCombination(self):
        for et in [self.eventtable_hostingcom]:
            colrulelist = []
            colrulelist.append(PopulatedRowCountDS(et.getAllColsAsDict(), et.getCategoricalCols(), et.getNumericalCols()))
            self.columnRuleTestAlgorithmMany(colrulelist, et.getDataFrame(), 'none')
