from abc import abstractmethod
from avro import schema, datafile, io
import codecs
from pipelinefwk import Pipeline
from pipelinefwk import PipelineStep

class DataRulePipeline(Pipeline):

    def __init__(self, pipelineSteps):
        super(DataRulePipeline, self).__init__(pipelineSteps)

    def apply(self, dataFrame, configMetadata):
        for step in self.pipelineSteps:
            step.apply(dataFrame, configMetadata)
        return dataFrame

    def processResults(self, dataRulesLocalDir):
        avroSchema = getSchema()

        for step in self.pipelineSteps:
            fileSuffix = ""
            if isinstance(step, ColumnRule):
                fileSuffix = "Column"
                results = step.getColumnsToRemove()
            elif isinstance(step, RowRule):
                fileSuffix = "Row"
                results = step.getRowsToRemove()
            elif isinstance(step, TableRule):
                fileSuffix = "Table"
                results = step.getRowsToRemove()

            recordWriter = io.DatumWriter(avroSchema)
            outputFileName = step.__class__.__name__ + '_' + fileSuffix + '.avro'
            dataWriter = datafile.DataFileWriter(codecs.open(dataRulesLocalDir + outputFileName, 'wb'),
                                                 recordWriter, writers_schema=avroSchema, codec='deflate')

            index = 1
            for itemId, toRemove in results.iteritems():
                if toRemove:
                    datum = {}
                    datum["id"] = index
                    datum["itemid"] = itemId
                    index = index + 1
                    dataWriter.append(datum)

            dataWriter.close()

def getSchema():
    '''
    Returns the schema of output avro file
    Args:
        None
    Returns:
        Hardcoded schema
    '''

    ruleSchema = """
    {
      "type" : "record",
      "name" : "RuleOutput",
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
      "tableName" : "RuleOutput"
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
