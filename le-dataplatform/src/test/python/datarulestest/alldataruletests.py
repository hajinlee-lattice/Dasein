
# $LastChangedBy$
# $LastChangedDate$
# $Rev$

import logging
import pandas as pd

from dataruleeventtable import DataRuleEventTable
from datarules.nullissue import NullIssue
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
            self.logger.info('* Parameter {0:20s} = {1}'.format(p,v))
        colrule.apply(dataframe, dictOfArguments)
        for c, r in colrule.getResults().iteritems():
            if not r.isPassed() and reportset in ['all', 'failed']:
                self.logger.info('! Failed Column {0}: {1}'.format(c,r.getMessage()))
            elif reportset in ['all', 'passed']:
                self.logger.info('o Passed Column {0}: {1}'.format(c,r.getMessage()))
