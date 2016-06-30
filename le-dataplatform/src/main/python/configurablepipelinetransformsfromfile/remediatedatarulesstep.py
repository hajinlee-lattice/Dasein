import fastavro as avro
import glob
import os
import pwd
from urlparse import urlparse

from pipelinefwk import PipelineStep
from pipelinefwk import get_logger
from webhdfs import WebHDFS

logger = get_logger("pipeline")

class RemediateDataRulesStep(PipelineStep):

    def __init__(self, params, enabledRules=[]):
        self.enabledRules = enabledRules
        self.params = params
        logger.info('Enabled rules: %s' % ' '.join(self.enabledRules))
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


    def transform(self, dataFrame, configMetadata, test):
        idColumn = None
        if 'Id' in dataFrame.columns:
            idColumn = 'Id'
        elif 'LeadID' in dataFrame.columns:
            idColumn = 'LeadID'
        elif 'ExternalId' in dataFrame.columns:
            idColumn = 'ExternalId'

        if not idColumn:
            logger.info('No id column identified; not removing any rows')
        else:
            rowsToRemove = set()
            for filename in glob.glob(os.path.join("./", "*RowRule.avro")):
                rowsToRemove.update(self.getItemsToRemove(filename))

            for filename in glob.glob(os.path.join("./", "*TableRule.avro")):
                rowsToRemove.update(self.getItemsToRemove(filename))

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

        if self.enabledRules and ruleName in self.enabledRules:
            logger.info('Processing enabled rule %s' % ruleName)
        else:
            logger.info('Skipping disabled rule %s' % ruleName)
            return set()

        items = set()
        if os.path.isfile(filename):
            with open(filename) as fp:
                reader = avro.reader(fp)
                for record in reader:
                    items.add(record["itemid"])

        return items

    def stripPath(self, fileName):
        return fileName[fileName.rfind("/") + 1:len(fileName)]

