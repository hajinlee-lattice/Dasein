import fastavro as avro
import glob
import os
import pwd
from urlparse import urlparse

from pipelinefwk import PipelineStep
from pipelinefwk import get_logger
from webhdfs import WebHDFS

logger = get_logger("remediatedatarules")

class RemediateDataRulesStep(PipelineStep):

    def __init__(self, params, enabledRules={}):
        self.enabledRules = enabledRules
        self.params = params
        self.columnRules = {}
        self.rowRules = {}

        logger.info('Enabled rules: %s' % str(self.enabledRules))
        if params:
            rulesPath = params["schema"]["datarules_path"] + 'datarules/'
            logger.info('datarules path %s' % params["schema"]["datarules_path"])
            webHdfsHostPort = urlparse(os.environ['SHDP_HD_FSWEB'])
            hdfs = WebHDFS(webHdfsHostPort.hostname, webHdfsHostPort.port, pwd.getpwuid(os.getuid())[0])

            try:
                ruleFiles = hdfs.listdir(rulesPath)
            except:
                logger.error("Cannot list rules directory %s" % rulesPath)
                return

            for ruleFileHdfs in ruleFiles:
                ruleFileHdfs = rulesPath + ruleFileHdfs
                ruleFileLocal = os.getcwd() + '/' + self.stripPath(ruleFileHdfs)
                try:
                    logger.info("Copying HDFS rule file %s to local %s." % (ruleFileHdfs, ruleFileLocal))
                    hdfs.copyToLocal(ruleFileHdfs, ruleFileLocal)
                except:
                    logger.error("Cannot copy HDFS rule file %s to local %s." % (ruleFileHdfs, ruleFileLocal))

            for filename in glob.glob(os.path.join("./", "*Rule.avro")):
                logger.info(filename)

            self.populateRules()

    def populateRules(self):
        for filename in glob.glob(os.path.join("./", "*ColumnRule.avro")):
            ruleName, items = self.getItemsToRemove(filename)
            self.columnRules[ruleName] = items

        for filename in glob.glob(os.path.join("./", "*RowRule.avro")):
            ruleName, items = self.getItemsToRemove(filename)
            self.rowRules[ruleName] = items

        for filename in glob.glob(os.path.join("./", "*TableRule.avro")):
            ruleName, items = self.getItemsToRemove(filename)
            self.rowRules[ruleName] = items

        logger.info('Preloaded Column rule results:%s' % str(self.columnRules))
        logger.info('Preloaded Row rule results:%s' % str(self.rowRules))

    def transform(self, dataFrame, configMetadata, test):
        columnsToRemove = set()
        rowsToRemove = set()

        for ruleName, items in self.enabledRules.items():
            itemsToRemove = []
            if self.columnRules.has_key(ruleName):
                if items:
                    itemsToRemove = items
                else:
                    itemsToRemove = self.columnRules[ruleName]
                columnsToRemove.update(itemsToRemove)
            elif self.rowRules.has_key(ruleName):
                if items:
                    itemsToRemove = items
                else:
                    itemsToRemove = self.rowRules[ruleName]
                rowsToRemove.update(itemsToRemove)

            logger.info('Items to remove for rule %s: %s' % (ruleName, str(itemsToRemove)))

        if columnsToRemove:
            logger.info("Removing columns %s" % columnsToRemove)
            logger.info('Number of columns before rule remediation: %d' % len(dataFrame.columns))
            super(RemediateDataRulesStep, self).removeColumns(dataFrame, columnsToRemove)
            logger.info('Number of columns after rule remediation: %d' % len(dataFrame.columns))
        else:
            logger.info("No columns to remove")

        idColumn = self.params["idColumn"]

        if not idColumn:
            logger.info('No id column identified; not removing any rows')
        else:
            if rowsToRemove:
                logger.info('id column name: ' + idColumn)
                logger.info('Number of rows before rule remediation: %d' % len(dataFrame.index))
                logger.info("Removing rows %s" % rowsToRemove)
                for r in rowsToRemove:
                    dataFrame.drop(dataFrame.index[dataFrame[idColumn] == r], inplace=True)

                dataFrame.reset_index(drop=True, inplace=True)
                logger.info('Number of rows after rule remediation: %d' % len(dataFrame.index))
            else:
                logger.info("No rows to remove")

        if test:
            self.params["testDataRemediated"] = dataFrame.copy(deep=True)
        else:
            self.params["trainingDataRemediated"] = dataFrame.copy(deep=True)

        return dataFrame

    def getItemsToRemove(self, filename):
        filenameOnly = self.stripPath(filename)
        ruleName = filenameOnly[:filenameOnly.find('_')]

        items = set()
        if os.path.isfile(filename):
            with open(filename) as fp:
                reader = avro.reader(fp)
                for record in reader:
                    items.add(record["itemid"])

        return ruleName, items

    def stripPath(self, fileName):
        return fileName[fileName.rfind("/") + 1:len(fileName)]

