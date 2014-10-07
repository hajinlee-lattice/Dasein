import codecs
from collections import OrderedDict
import os
import pwd
import json
import logging
import math
import sys

from avro import schema, datafile, io
from sklearn import metrics
from sklearn.metrics.cluster.supervised import entropy

from leframework.bucketers.bucketerdispatcher import BucketerDispatcher
from leframework.executors.dataprofilingexecutor import DataProfilingExecutor
from leframework.progressreporter import ProgressReporter
import numpy as np


reload(sys)
sys.setdefaultencoding('utf-8')


logging.basicConfig(level=logging.INFO, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='data_profile')

def getExecutor():
    return DataProfilingExecutor()

def getSchema():
    '''
    Returns the schema of output avro file
    Args:
        None
    Returns: 
        Hardcoded schema
    '''
    
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
        "sqlType" : "8"
      }, {
        "name" : "count",
        "type" : [ "int", "null" ],
        "columnName" : "count",
        "sqlType" : "4"
      }, {
        "name" : "lift",
        "type" : [ "double", "null" ],
        "columnName" : "lift",
        "sqlType" : "8"
      }, {
        "name" : "uncertaintyCoefficient",
        "type" : [ "double", "null" ],
        "columnName" : "uncertaintyCoefficient",
        "sqlType" : "8"
      }, {
        "name" : "discreteNullBucket",
        "type" : [ "boolean", "null" ],
        "columnName" : "discreteNullBucket",
        "sqlType" : "16"
      }, {
        "name" : "continuousNullBucket",
        "type" : [ "boolean", "null" ],
        "columnName" : "continuousNullBucket",
        "sqlType" : "16"
      }, {
        "name" : "nullCount",
        "type" : [ "int", "null" ],
        "columnName" : "nullCount",
        "sqlType" : "4"
      }  ],
      "tableName" : "EventMetadata"
    }"""
    return schema.parse(metadataSchema)

def train(trainingData, testData, schema, modelDir, algorithmProperties, runtimeProperties = None):
    '''
    Profiles each feature column in the entire data set and performs bucketing on band columns
    Args:
        trainingData: DataFrame object of training data
        testData: DataFrame object of test data
        schema: A dictionary containing necessary metadata
        modelDir: Output directory
        algorithmProperties: Unused
        runtimeProperties: Contains properties to report progress update
    Output: 
        profile.avro: Profiling information used by modeling
        diagnostics.json: Diagnostics about the data set and bucketing metadata
    '''
    if runtimeProperties is not None:
        # Set up progress reporter for data profiling
        progressReporter = ProgressReporter(runtimeProperties["host"], int(runtimeProperties["port"]))
        progressReporter.inStateMachine()
    else:
        # progressReporter disabled
        progressReporter = ProgressReporter(None, 0)

    data = trainingData.append(testData)
    bucketDispatcher = BucketerDispatcher()
    avroSchema = getSchema()
    recordWriter = io.DatumWriter(avroSchema)
    dataWriter = datafile.DataFileWriter(codecs.open(modelDir + '/profile.avro', 'wb'),
                                         recordWriter, writers_schema = avroSchema, codec = 'deflate')

    colnames = list(data.columns.values)
    stringcols = set(schema["stringColumns"])
    features = set(schema["features"])
    eventVector = data.iloc[:,schema["targetIndex"]]
    colnameBucketMetadata, metadataDiagnostics = retrieveColumnBucketMetadata(schema["config_metadata"])
    progressReporter.setTotalState(len(colnames))

    index = 1
    dataDiagnostics = []
    for colname in colnames:
        # Update progress
        progressReporter.nextState()
        if colname in features:
            index, columnDiagnostics = profileColumn(data[colname], colname, stringcols, eventVector, bucketDispatcher, dataWriter, index, colnameBucketMetadata.get(colname))
            dataDiagnostics.append(columnDiagnostics)

    writeDiagnostics(dataDiagnostics, metadataDiagnostics, eventVector, features, modelDir)
    dataWriter.close()
    return None

def retrieveColumnBucketMetadata(columnsMetadata):
    '''
    Reads DisplayDiscretizationStrategy as bucketing strategy for each column and diagnoses any invalid metadata input to any columns
    Args:
        columnsMetadata: A dictionary mapping columns to its metadata
    Returns:
        bucketsMetadata: A dictionary mapping columns to bucketing strategy, i.e., (columnName, [bucketing_type, bucketing_parameters])
        diagnostics: A dictionary mapping invalid columns to its invalid metadata
    Raises:
        RuntimeError : An error occurred when more than 1 bucketing strategy is found for a given column
    '''
    bucketsMetadata = dict()
    diagnostics = OrderedDict()
    if columnsMetadata is None or not columnsMetadata.has_key("Metadata"):
        diagnostics["Summary"] = "Invalid metadata format"
        return (bucketsMetadata, diagnostics)
    else:
        columnsMetadata = columnsMetadata["Metadata"]

    for columnMetadata in columnsMetadata:
        try :
            if columnMetadata['DisplayDiscretizationStrategy'] is None:
                continue
            bucketMetadata = json.loads(columnMetadata['DisplayDiscretizationStrategy'])
        except :
            logger.warn("Invalid metadata format for column: " + columnMetadata['ColumnName'])
            # Include column metadata in diagnostics 
            diagnostics[columnMetadata['ColumnName']] = columnMetadata
            continue

        if len(bucketMetadata) != 1:
            raise RuntimeError("Only one bucketing strategy is allowed.")

        for key, value in bucketMetadata.iteritems():
            bucketsMetadata[columnMetadata['ColumnName']] = [key, value]

    return (bucketsMetadata, diagnostics)

def getPopulatedRowCount(columnData, continuous):
    if continuous:
        return columnData.count()
    return sum(map(lambda x: 1 if x is not None else 0, columnData))

def profileColumn(columnData, colname, stringcols, eventVector, bucketDispatcher, dataWriter, index, bucketingParams = None):
    '''
    Performs profiling on given column 
    Args:
        columnData: A DataFrame vector of data for given column
        colname: Name of given column
        stringcols: A list of names of string columns 
        eventVector: A DataFrame vector of event column
        bucketDispatcher: A dispatcher that performs specific bucketing based on passed in parameters
        dataWriter: A buffered writer that writes to profile.avro
        index: Current id of column in output file
        bucketingParams: Parameters for bucketing
    Returns:
        index: Id of next column in avro file 
        diagnostics: A dictionary of summary information of each column, i.e., PopulationRate, BucketingStrategy
        
    Raises:
        RuntimeError : An error occurred when more than 1 bucketing strategy is found for a given column
    '''
    diagnostics = OrderedDict()
    diagnostics["Colname"] = colname
    diagnostics["PopulationRate"] = getPopulatedRowCount(columnData, colname not in stringcols)/float(len(columnData))
    
    if diagnostics["PopulationRate"] == 0.0:
        return (index, diagnostics)

    if colname in stringcols:
        # Categorical column
        diagnostics["Type"] = "Categorical"
        uniqueValues = len(columnData.unique())
        mode = columnData.value_counts().idxmax()
        diagnostics["UniqueValues"] = uniqueValues
        if uniqueValues > 200:
            logger.warn("String column name: " + colname + " is discarded due to more than 200 unique values.")
            return (index, diagnostics)

        index = writeCategoricalValuesToAvro(dataWriter, columnData, eventVector, mode, colname, index)
    else:
        # Band column
        diagnostics["Type"] = "Band"
        mean = columnData.mean()
        median = columnData.median()
        if math.isnan(median):
            logger.warn("Median to impute for column name: " + colname + " is null; excluding this column.")
            return (index, diagnostics)
        # Convert all continuous values into a numeric data type
        columnData = columnData.convert_objects(convert_numeric=True) 
        if bucketingParams is not None:
            # Apply bucketing with specified type and parameters
            bands = bucketDispatcher.bucketColumn(columnData, eventVector, bucketingParams[0], bucketingParams[1])
            diagnostics["BucketingStrategy"] = bucketingParams
        else:
            # Default bucketer
            logger.debug("Using default bucketer for column name: " + colname)
            bands = bucketDispatcher.bucketColumn(columnData, eventVector)
            diagnostics["BucketingStrategy"] = None
        index = writeBandsToAvro(dataWriter, columnData, eventVector, bands, mean, median, colname, index)

    return (index, diagnostics)


def writeCategoricalValuesToAvro(dataWriter, columnVector, eventVector, mode, colname, index):
    '''
    Creates a datum for each unique value in the categorical column and writes to buffered writer   
    Args:
        dataWriter: Buffered writer which appends each datum to the avro file
        columnVector: A DataFrame vector of column data 
        eventVector: A DataFrame vector of event column
        mode: Mode of all values in the column vector
        colname: Name of given column
        index: Current id of column in output file
    Returns:
        index: id of next column in output file 
    '''
    avgProbability = sum(eventVector) / float(len(eventVector))
    for value in columnVector.unique():
        valueVector = map(lambda x: 1 if x == value else 0, columnVector)
        valueCount = sum(valueVector)
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
        datum["lift"] = getLift(avgProbability, valueCount, valueVector, eventVector)
        datum["uncertaintyCoefficient"] = uncertaintyCoefficientXgivenY(eventVector, valueVector)
        datum["discreteNullBucket"] = False
        datum["continuousNullBucket"] = False
        index = index + 1
        dataWriter.append(datum)

    # Create bucket for nulls if applicable
    index = writeNullBucket(index, colname, columnVector, eventVector, avgProbability, dataWriter, False)
    return index

def writeBandsToAvro(dataWriter, columnVector, eventVector, bands, mean, median, colname, index):
    '''
    Creates a datum for each band in the band column and writes to buffered writer   
    Args:
        dataWriter: Buffered writer which appends each datum to the avro file
        columnVector: A DataFrame vector of column data 
        eventVector: A DataFrame vector of event column
        bands: A list of band boundries, i.e. band i = (band[i], band[i+1)
        colname: Name of given column
        index: Current id of column in output file
    Returns:
        index: id of next column in output file 
    '''
    avgProbability = sum(eventVector) / float(len(eventVector))
    for i in range(len(bands) - 1):
        bandVector = map(lambda x: 1 if x >= bands[i] and x < bands[i + 1] else 0, columnVector)
        bandCount = sum(bandVector)
        if bandCount == 0:
            logger.critical("No samples found in band [" + str(bands[i]) + ", " + str(bands[i+1]) + "] for column: " + colname)
            continue

        # Replace np.inf with None value
        band = map(lambda x: None if np.isinf(x) else x, [bands[i], bands[i + 1]])
        datum = {}
        datum["id"] = index
        datum["barecolumnname"] = colname
        datum["columnvalue"] = None
        datum["Dtype"] = "BND"
        datum["minV"] = band[0]
        datum["maxV"] = band[1]
        datum["mean"] = mean
        datum["median"] = median
        datum["mode"] = None
        datum["count"] = bandCount
        datum["lift"] = getLift(avgProbability, bandCount, bandVector, eventVector)
        datum["uncertaintyCoefficient"] = uncertaintyCoefficientXgivenY(eventVector, bandVector)
        datum["discreteNullBucket"] = False
        datum["continuousNullBucket"] = False
        index = index + 1
        dataWriter.append(datum)

    # Create bucket for nulls if applicable
    index = writeNullBucket(index, colname, columnVector, eventVector, avgProbability, dataWriter, True)
    return index

def writeNullBucket(index, colname, columnVector, eventVector, avgProbability, dataWriter, continuous):
    bandVector = []
    
    if continuous:
        bandVector = map(lambda x: 1 if np.isnan(x) else 0, columnVector)
    else:
        bandVector = map(lambda x: 1 if x is None else 0, columnVector)
    bandCount = sum(bandVector)
    if bandCount == 0:
        return index
    
    datum = {}
    datum["id"] = index
    datum["barecolumnname"] = colname
    datum["columnvalue"] = None
    datum["Dtype"] = "BND" if continuous else "STR"
    datum["minV"] = None
    datum["maxV"] = None
    datum["mean"] = None
    datum["median"] = None
    datum["mode"] = None
    datum["count"] = bandCount
    datum["lift"] = getLift(avgProbability, bandCount, bandVector, eventVector)
    datum["uncertaintyCoefficient"] = uncertaintyCoefficientXgivenY(eventVector, bandVector)
    datum["discreteNullBucket"] = not continuous
    datum["continuousNullBucket"] = continuous
    index = index + 1
    dataWriter.append(datum)
    return index

def writeDiagnostics(dataDiagnostics, metadataDiagnostics, eventVector, features, modelDir):
    '''
    Writes all diagnostics to a json file   
    Args:
        dataDiagnostics: A dictionary of diagnostics on the data set
        metadataDiagnostics: A dictionary of diagnostics on the metadata for bucketing strategies 
        eventVector: A DataFrame vector of event column
        features: A list of feature column names
        modelDir: Output directory of the json file
    Returns:
        None 
    '''
    summary = OrderedDict()
    summary["SampleSize"] = len(eventVector)
    summary["ColumnSize"] = len(features)
    summary["PositiveEventRate"] = sum(eventVector)/float(len(eventVector))

    diagnostics = OrderedDict()
    diagnostics["Summary"] = summary
    diagnostics["MetadataDiagnostics"] = metadataDiagnostics
    diagnostics["ColumnDiagnostics"] = dataDiagnostics
    with open(modelDir + "diagnostics.json", "wb") as fp:
        json.dump(diagnostics, fp)


def getCountWhereEventIsOne(valueVector, eventVector):
    '''
    Finds the count of rows where value and event are both 1   
    Args:
        valueVector: A DataFrame vector of boolean values where valueVector[i] = 1 means value = x for row i
        eventVector: A DataFrame vector of event column
    Returns:
        The final count value 
    '''
    counter = lambda x, y: 1 if x == 1 and y == 1 else 0
    return sum(map(counter, valueVector, eventVector))

def getLift(avgProbability, valueCount, valueVector, eventVector):
    '''
    Calculates the lift of a given value x of a given column based on the formula:
    lift = P(Event = 1 | Value = x) / P(Event = 1)
    Args:
        avgProbability: A float value which is P(Event = 1)
        valueCount: Count of rows where value = x for a given column
        valueVector: A DataFrame vector of boolean values where valueVector[i] = 1 means value = x for row i
        eventVector A DataFrame vector of event column
    Returns:
        Lift value
    '''
    if (avgProbability * valueCount) == 0:
        return None
    return getCountWhereEventIsOne(valueVector, eventVector) / float(avgProbability * valueCount)

def uncertaintyCoefficientXgivenY(x, y):
    '''
    Calculates the uncertaintyCoefficient of X given Y.
    In this case, x should be the event column, while y should be the predictor column-value
    Args:
        x: A DataFrame vector
        y: A DataFrame vector
    Returns:
        UncertaintyCefficient value
    '''
    if entropy(x) == 0:
        return None
    return metrics.mutual_info_score(x, y) / entropy(x)

