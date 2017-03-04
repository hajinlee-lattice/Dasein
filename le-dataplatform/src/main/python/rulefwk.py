from abc import abstractmethod
from avro import schema, datafile, io
import codecs
import logging

from pipelinefwk import Pipeline
from pipelinefwk import PipelineStep


logger = logging.getLogger(name='rulefwk')

class DataRulePipeline(Pipeline):

    def __init__(self, pipelineSteps):
        super(DataRulePipeline, self).__init__(pipelineSteps)

    def apply(self, dataFrame, columnMetadata, profile):
        for step in self.pipelineSteps:
            try:
                logger.info("Applying DataRule " + step.__class__.__name__)
                step.apply(dataFrame, columnMetadata, profile)
            except Exception as e:
                logger.exception("Caught Exception while applying datarule. Stack trace below" + str(e))
        return dataFrame

    def processResults(self, dataRulesLocalDir, dataFrame, targetColumn, idColumn=None):
        for step in self.pipelineSteps:
            logger.info("Processing results for DataRule " + step.__class__.__name__)
            step.logAndWriteResults(dataRulesLocalDir, dataFrame, targetColumn, idColumn)


class RuleResults(object):

    def __init__(self, ruleIsPassed, mesg, pardict):
        self.ruleIsPassed = ruleIsPassed
        self.mesg = mesg
        self.pardict = pardict

    def isPassed(self):
        return self.ruleIsPassed

    def getMessage(self):
        return self.mesg

    def getParDict(self):
        return self.pardict

class DataRule(PipelineStep):

    interfaceColumns = { 'Id',
                         'InternalId',
                         'Event',
                         'Domain',
                         'LastModifiedDate',
                         'CreatedDate',
                         'FirstName',
                         'LastName',
                         'Title',
                         'Email',
                         'City',
                         'State',
                         'PostalCode',
                         'Country',
                         'PhoneNumber',
                         'Website',
                         'CompanyName',
                         'Industry',
                         'LeadSource',
                         'IsClosed',
                         'StageName',
                         'AnnualRevenue',
                         'NumberOfEmployees',
                         'YearStarted' }

    def __init__(self, props):
        super(DataRule, self).__init__(props)
        self.fileSuffix = "AbstractRule"

    @abstractmethod
    def apply(self, dataFrame, columnMetadata, profile):
        pass

    @abstractmethod
    def getDescription(self):
        pass

    @abstractmethod
    def getConfParameters(self):
        pass

    @abstractmethod
    def getResults(self):
        pass

    @abstractmethod
    def getSchema(self):
        pass

    @abstractmethod
    def appendDataWriterRow(self, index, itemID, dataWriter, dataFrame, targetColumn, idColumn):
        pass

    def logAndWriteResults(self, dataRulesLocalDir, dataFrame, targetColumn, idColumn):
        results = self.getResults()
        if not results:
            logger.info("No DataRule results for " + self.__class__.__name__)
            return

        self.logResults(results)

        with self.getDataWriter(dataRulesLocalDir) as dataWriter:
            index = 1
            for itemID, ruleResult in results.iteritems():
                if not ruleResult.isPassed():
                    self.appendDataWriterRow(index, itemID, dataWriter, dataFrame, targetColumn, idColumn)
                    index += 1

    def logResults(self, results):
        details = ''
        n_cols = 0
        n_failed = 0
        for c, r in results.iteritems():
            n_cols += 1
            if not r.isPassed():
                n_failed += 1
                details = details + '{0:50s} FAILED: {1}\n'.format(c, r.getMessage())
            else:
                details = details + '{0:50s} PASSED: {1}\n'.format(c, r.getMessage())
        logger.info('DataRule results: {0} columns failed ({1:.2%})\n{2}'.format(n_failed, float(n_failed) / float(n_cols), details))

    def getDataWriter(self, dataRulesLocalDir):
        recordWriter = io.DatumWriter(self.getSchema())
        outputFileName = self.__class__.__name__ + '_' + self.fileSuffix + '.avro'
        dataWriter = datafile.DataFileWriter(codecs.open(dataRulesLocalDir + outputFileName, 'wb'),
                recordWriter, writers_schema=self.getSchema(), codec='deflate')
        return dataWriter



class ColumnRule(DataRule):

    def __init__(self, props):
        super(ColumnRule, self).__init__(props)
        self.fileSuffix = "ColumnRule"

    def getSchema(self):
        '''
        Returns the schema of column rule output avro file
        Args:
            None
        Returns:
            Hardcoded schema
        '''

        ruleSchema = """
        {
            "type" : "record",
            "name" : "ColumnRuleOutput",
            "doc" : "Rule output from data review",
            "fields" : [ {
                "name" : "Id",
                "type" : [ "int", "null" ],
                "columnName" : "id",
                "sqlType" : "4"
                }, {
                "name" : "ColumnName",
                "type" : [ "string", "null" ],
                "columnName" : "itemid",
                "sqlType" : "-9"
                } ],
            "tableName" : "ColumnRuleOutput"
        }"""
        return schema.parse(ruleSchema)

    def appendDataWriterRow(self, index, columnName, dataWriter, dataFrame, targetColumn, idColumn):
        datum = {}
        datum["Id"] = index
        datum["ColumnName"] = columnName
        dataWriter.append(datum)


class RowRule(DataRule):

    def __init__(self, props):
        super(RowRule, self).__init__(props)
        self.fileSuffix = "RowRule"

    def getSchema(self):
        '''
        Returns the schema of row rule output avro file
        Args:
            None
        Returns:
            Hardcoded schema
        '''

        ruleSchema = """
        {
            "type" : "record",
            "name" : "RowRuleOutput",
            "doc" : "Rule output from data review",
            "fields" : [ {
                "name" : "id",
                "type" : [ "int", "null" ],
                "columnName" : "id",
                "sqlType" : "4"
                }, {
                "name" : "itemid",
                "type" : [ "string", "null" ],
                "columnName" : "itemid",
                "sqlType" : "-9"
                }, {
                "name" : "isPositiveEvent",
                "type" : [ "boolean", "null" ],
                "columnName" : "itemid",
                "sqlType" : "16"
                }, {
                "name" : "columns",
                "type" : [ "string", "null" ],
                "columnName" : "columns",
                "sqlType" : "-9"
            }],
            "tableName" : "RowRuleOutput"
        }"""
        return schema.parse(ruleSchema)

    def appendDataWriterRow(self, index, rowId, dataWriter, dataFrame, targetColumn, idColumn):
        # # This needs implementation
        pass


class TableRule(RowRule):

    def __init__(self, props):
        super(TableRule, self).__init__(props)
        self.fileSuffix = "TableRule"
