from abc import abstractmethod
from avro import schema, datafile, io
import codecs
from pipelinefwk import Pipeline
from pipelinefwk import PipelineStep

class DataRulePipeline(Pipeline):

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

    def __init__(self, pipelineSteps):
        super(DataRulePipeline, self).__init__(pipelineSteps)

    def apply(self, dataFrame, configMetadata):
        for step in self.pipelineSteps:
            step.apply(dataFrame, configMetadata)
        return dataFrame

    def processResults(self, dataRulesLocalDir):
        for step in self.pipelineSteps:
            fileSuffix = ""
            if isinstance(step, ColumnRule):
                avroSchema = getColumnSchema()
                fileSuffix = "ColumnRule"
                results = step.getColumnsToRemove()
            elif isinstance(step, RowRule):
                avroSchema = getRowSchema()
                fileSuffix = "RowRule"
                results = step.getRowsToRemove()
            elif isinstance(step, TableRule):
                avroSchema = getRowSchema()
                fileSuffix = "TableRule"
                results = step.getRowsToRemove()

            if not results:
                continue

            recordWriter = io.DatumWriter(avroSchema)
            outputFileName = step.__class__.__name__ + '_' + fileSuffix + '.avro'
            dataWriter = datafile.DataFileWriter(codecs.open(dataRulesLocalDir + outputFileName, 'wb'),
                                                 recordWriter, writers_schema=avroSchema, codec='deflate')

            index = 1
            if isinstance(step, ColumnRule):
                for itemId, toRemove in results.iteritems():
                    if toRemove and itemId not in self.interfaceColumns:
                        datum = {}
                        datum["id"] = index
                        datum["itemid"] = itemId
                        index = index + 1
                        dataWriter.append(datum)
            else:
                for itemId, columns in results.iteritems():
                    datum = {}
                    datum["id"] = index
                    datum["itemid"] = itemId
                    datum["columns"] = ','.join(columns)
                    index = index + 1
                    dataWriter.append(datum)

            dataWriter.close()

def getColumnSchema():
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
        "name" : "id",
        "type" : [ "int", "null" ],
        "columnName" : "id",
        "sqlType" : "4"
      }, {
        "name" : "itemid",
        "type" : [ "string", "null" ],
        "columnName" : "itemid",
        "sqlType" : "-9"
      }],
      "tableName" : "ColumnRuleOutput"
    }"""
    return schema.parse(ruleSchema)

def getRowSchema():
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
        "name" : "columns",
        "type" : [ "string", "null" ],
        "columnName" : "columns",
        "sqlType" : "-9"
      }],
      "tableName" : "RowRuleOutput"
    }"""
    return schema.parse(ruleSchema)

class DataRule(PipelineStep):

    def __init__(self, props):
        super(DataRule, self).__init__(props)

    @abstractmethod
    def apply(self, dataFrame, configMetadata):
        return

    @abstractmethod
    def getDescription(self):
        return

class RowRule(DataRule):

    def __init__(self, props):
        super(RowRule, self).__init__(props)

    @abstractmethod
    def getRowsToRemove(self):
        return

class ColumnRule(DataRule):

    def __init__(self, props):
        super(ColumnRule, self).__init__(props)

    @abstractmethod
    def getColumnsToRemove(self):
        return

class TableRule(DataRule):

    def __init__(self, props):
        super(TableRule, self).__init__(props)

    @abstractmethod
    def getRowsToRemove(self):
        return
