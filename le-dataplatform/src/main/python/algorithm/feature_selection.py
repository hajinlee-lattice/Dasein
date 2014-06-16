from avro import schema, datafile, io
import codecs
import re
import sys

from leframework.executors.dataprofilingexecutor import DataProfilingExecutor
import pandas as pd


reload(sys)
sys.setdefaultencoding('utf-8')



def getExecutor():
    return DataProfilingExecutor()

def getSchema():
    metadataSchema = """
    {
      "type" : "record",
      "name" : "EventMetadata",
      "doc" : "Metadata from data profiling",
      "fields" : [ {
        "name" : "id",
        "type" : [ "int", "null" ],
        "columnName" : "id",
        "sqlType" : "4"
      }, {
        "name" : "barecolumnname",
        "type" : [ "string", "null" ],
        "columnName" : "barecolumnname",
        "sqlType" : "-9"
      }, {
        "name" : "columnvalue",
        "type" : [ "string", "null" ],
        "columnName" : "columnvalue",
        "sqlType" : "-9"
      }, {
        "name" : "Dtype",
        "type" : [ "string", "null" ],
        "columnName" : "Dtype",
        "sqlType" : "-9"
      }, {
        "name" : "maxV",
        "type" : [ "double", "null" ],
        "columnName" : "maxV",
        "sqlType" : "8"
      }, {
        "name" : "minV",
        "type" : [ "double", "null" ],
        "columnName" : "minV",
        "sqlType" : "8"
      }, {
        "name" : "mode",
        "type" : [ "string", "null" ],
        "columnName" : "mode",
        "sqlType" : "-9"
      }, {
        "name" : "mean",
        "type" : [ "double", "null" ],
        "columnName" : "mean",
        "sqlType" : "4"
      }, {
        "name" : "median",
        "type" : [ "double", "null" ],
        "columnName" : "median",
        "sqlType" : "4"
      } ],
      "tableName" : "EventMetadata"
    }"""
    return schema.parse(metadataSchema)

def train(trainingData, testData, schema, modelDir, algorithmProperties):
    data = trainingData.append(testData)

    avroSchema = getSchema()
    recordWriter = io.DatumWriter(avroSchema)
    print(sys.getdefaultencoding())
    dataWriter = datafile.DataFileWriter(codecs.open(modelDir + '/metadata.avro', 'wb'), recordWriter, writers_schema = avroSchema, codec = 'deflate')
    
    colnames = list(data.columns.values)
    stringcols = set(schema["stringColumns"])
    features = set(schema["features"])
    index = 1
    for colname in colnames:
        if colname not in features:
            continue
        # Categorical column
        if colname in stringcols:
            uniquevalues = data[colname].unique()
            mode = data[colname].value_counts().idxmax()
            index = writeCategoricalValuesToAvro(dataWriter, uniquevalues, mode, colname, index)
        else:
            mean = data[colname].mean()
            median = data[colname].median()
            try:
                bands = pd.cut(data[colname].fillna(median), 5).levels
                index = writeBandsToAvro(dataWriter, bands, mean, median, colname, index)
            except Exception:
                continue
    dataWriter.close()
    return None

def writeCategoricalValuesToAvro(dataWriter, uniquevalues, mode, colname, index):
    for value in uniquevalues:
        if len(uniquevalues) > 1 and value is None:
            continue
        datum = {}
        datum["id"] = index
        datum["barecolumnname"] = colname
        datum["columnvalue"] = value
        datum["Dtype"] = "STR"
        datum["minV"] = None
        datum["maxV"] = None
        datum["mean"] = None
        datum["median"] = None
        datum["mode"] = mode
        index = index+1
        dataWriter.append(datum)
    
    return index

def writeBandsToAvro(dataWriter, bands, mean, median, colname, index):
    for band in bands:
        regexp = ",|\(|\]| "
        strs = re.split(regexp, band)
        datum = {}
        datum["id"] = index
        datum["barecolumnname"] = colname
        datum["columnvalue"] = None
        datum["Dtype"] = "BND"
        datum["minV"] = float(strs[1])
        datum["maxV"] = float(strs[3])
        datum["mean"] = mean
        datum["median"] = median
        datum["mode"] = None
        index = index+1
        dataWriter.append(datum)
    
    return index

    