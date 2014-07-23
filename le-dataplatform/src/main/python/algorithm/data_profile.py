from avro import schema, datafile, io
import codecs
import json
from sklearn import metrics
from sklearn.metrics.cluster.supervised import entropy
import sys

from leframework.bucketers.bucketerdispatcher import BucketerDispatcher
from leframework.executors.dataprofilingexecutor import DataProfilingExecutor
from leframework.progressreporter import ProgressReporter


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
      }, {
        "name" : "count",
        "type" : [ "int", "null" ],
        "columnName" : "count",
        "sqlType" : "4"
      }, {
        "name" : "lift",
        "type" : [ "double", "null" ],
        "columnName" : "lift",
        "sqlType" : "4"
      }, {
        "name" : "uncertaintyCoefficient",
        "type" : [ "double", "null" ],
        "columnName" : "uncertaintyCoefficient",
        "sqlType" : "4"
      }  ],
      "tableName" : "EventMetadata"
    }"""
    return schema.parse(metadataSchema)

def train(trainingData, testData, schema, modelDir, algorithmProperties, runtimeProperties = None):
    if runtimeProperties is not None:
        # Set up progress reporter for data profiling
        progressReporter = ProgressReporter(runtimeProperties["host"], int(runtimeProperties["port"]))
        progressReporter.inStateMachine()
    else:
        # progressReporter disabled
        progressReporter = ProgressReporter(None, 0)

    data = trainingData.append(testData)
    avroSchema = getSchema()
    bucketDispatcher = BucketerDispatcher()
    recordWriter = io.DatumWriter(avroSchema)
    print(sys.getdefaultencoding())
    dataWriter = datafile.DataFileWriter(codecs.open(modelDir + '/profile.avro', 'wb'),
                                         recordWriter, writers_schema = avroSchema, codec = 'deflate')

    colnames = list(data.columns.values)
    stringcols = set(schema["stringColumns"])
    features = set(schema["features"])
    eventVector = data[list(schema["targets"])[0]]
    colnameBucketMetadata = retrieveColumnBucketMetadata(schema["config_metadata"])
    index = 1
    progressReporter.setTotalState(len(colnames))

    for colname in colnames:
        progressReporter.nextState()
        if colname not in features:
            continue
        # Categorical column
        if colname in stringcols:
            # Impute null value
            data[colname] = data[colname].fillna('__unknown__')
            mode = data[colname].value_counts().idxmax()
            index = writeCategoricalValuesToAvro(dataWriter, data[colname], eventVector, mode, colname, index)
        else:
            mean = data[colname].mean()
            median = data[colname].median()
            # Impute null value
            data[colname] = data[colname].fillna(median)
            try:
                if colnameBucketMetadata.has_key(colname):
                    bands = bucketDispatcher.bucketColumn(data[colname], eventVector, colnameBucketMetadata[colname][0], colnameBucketMetadata[colname][1])
                else:
                    # default bucketer
                    bands = bucketDispatcher.bucketColumn(data[colname], eventVector)
                index = writeBandsToAvro(dataWriter, data[colname], eventVector, bands, mean, median, colname, index)
            except Exception as e:
                print e
                continue
    dataWriter.close()
    return None

def retrieveColumnBucketMetadata(columnsMetadata):
    '''
    Returns a dictionary of key: columnName, value: [bucketing_type, bucketing_parameters]
    '''
    bucketsMetadata = dict()
    if columnsMetadata is None or not columnsMetadata.has_key("Metadata"):
        return bucketsMetadata
    else:
        columnsMetadata = columnsMetadata["Metadata"]


    for columnMetadata in columnsMetadata:
        if not columnMetadata.has_key('DisplayDiscretizationStrategy'):
            continue
        
        if columnMetadata['DisplayDiscretizationStrategy'] is None:
            continue
        

        bucketMetadata = json.loads(columnMetadata['DisplayDiscretizationStrategy'])

        if len(bucketMetadata) != 1:
            raise RuntimeError("Only one bucketing strategy is allowed.")

        for key, value in bucketMetadata.iteritems():
            bucketsMetadata[columnMetadata['ColumnName']] = [key, value]

    return bucketsMetadata


def writeCategoricalValuesToAvro(dataWriter, columnVector, eventVector, mode, colname, index):
    uniquevalues = columnVector.unique()
    avgProbability = sum(eventVector) / float(len(eventVector))
    if len(uniquevalues) > 200:
        return index
    for value in uniquevalues:
        valueVector = map(lambda x: 1 if x == value else 0, columnVector)
        valueCount = getCountWhereEventIsOne(valueVector, eventVector)
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
        datum["count"] = valueCount
        datum["lift"] = valueCount / (avgProbability * sum(valueVector))
        datum["uncertaintyCoefficient"] = uncertaintyCoefficientXgivenY(eventVector, valueVector)
        index = index + 1
        dataWriter.append(datum)

    return index

def writeBandsToAvro(dataWriter, columnVector, eventVector, bands, mean, median, colname, index):
    # Set +/- infinity to null
    bands[0] = None
    bands[-1] = None
    avgProbability = sum(eventVector) / float(len(eventVector))
    for i in range(len(bands) - 1):
        bandVector = map(lambda x: 1 if x >= bands[i] and x < bands[i + 1] else 0, columnVector)
        bandCount = getCountWhereEventIsOne(bandVector, eventVector)
        datum = {}
        datum["id"] = index
        datum["barecolumnname"] = colname
        datum["columnvalue"] = None
        datum["Dtype"] = "BND"
        datum["minV"] = bands[i]
        datum["maxV"] = bands[i + 1]
        datum["mean"] = mean
        datum["median"] = median
        datum["mode"] = None
        datum["count"] = bandCount
        datum["lift"] = bandCount / (avgProbability * sum(bandVector))
        datum["uncertaintyCoefficient"] = uncertaintyCoefficientXgivenY(eventVector, bandVector)
        index = index + 1
        dataWriter.append(datum)

    return index

def getCountWhereEventIsOne(valueData, eventData):
    counter = lambda x, y: 1 if x == 1 and y == 1 else 0
    return sum(map(counter, valueData, eventData))

def uncertaintyCoefficientXgivenY(x, y):
    '''
      Given y, what parts of x can we predict.
      In this case, x should be the event column, while y should be the predictor column-value
    '''
    return metrics.mutual_info_score(x, y) / entropy(x)
