from avro import schema, datafile, io
import codecs
from collections import OrderedDict
import json
import logging
import math
from pandas.core.common import isnull
import sys

from scipy import stats

from leframework.bucketers.bucketerdispatcher import BucketerDispatcher
from leframework.executors.dataprofilingexecutor import DataProfilingExecutor
from leframework.progressreporter import ProgressReporter
import numpy as np
import pandas as pd

from leframework.bucketers.numericalbucketer import getBucketsAndStats
from leframework.bucketers.categoricalbucketer import getCatGroupingAndStatsForModel, getCatGroupingAndStatsForDisplay

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
        "name" : "displayname",
        "type" : [ "string", "null" ],
        "columnName" : "displayname",
        "sqlType" : "-9"
      }, {
        "name" : "approvedusage",
        "type" : [ "string", "null" ],
        "columnName" : "approvedusage",
        "sqlType" : "-9"
      }, {
        "name" : "category",
        "type" : [ "string", "null" ],
        "columnName" : "category",
        "sqlType" : "-9"
      }, {
        "name" : "fundamentaltype",
        "type" : [ "string", "null" ],
        "columnName" : "fundamentaltype",
        "sqlType" : "-9"
      },{
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
        "name" : "kurtosis",
        "type" : [ "double", "null" ],
        "columnName" : "kurtosis",
        "sqlType" : "8"
      }, {
        "name" : "skewness",
        "type" : [ "double", "null" ],
        "columnName" : "skewness",
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
      }, {
        "name" : "positiveEventCount",
        "type" : [ "int", "null" ],
        "columnName" : "positiveEventCount",
        "sqlType" : "4"
      }],
      "tableName" : "EventMetadata"
    }"""
    return schema.parse(metadataSchema)

def train(trainingData, testData, schema, modelDir, algorithmProperties, runtimeProperties=None, params=None):
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
    dataWriterModel = datafile.DataFileWriter(codecs.open(modelDir + '/model_profile.avro', 'wb'),
                                         recordWriter, writers_schema=avroSchema, codec='deflate')
    dataWriterUI = datafile.DataFileWriter(codecs.open(modelDir + '/profile.avro', 'wb'),
                                         recordWriter, writers_schema=avroSchema, codec='deflate')
    dataWriterFull={'Model':dataWriterModel,'UI':dataWriterUI}
    
    colNames = list(data.columns.values)
    categoricalCols = set(schema["stringColumns"])
    features = set(schema["features"])
    eventVector = data[schema["target"]]
    configMetadata = schema["config_metadata"]


    attributeStats = {"ApprovedUsage_Model":[], "ApprovedUsage_EmptyOrUnrecognized":[], "NULLDisplayName":[],
                      "NULLCategory":[], "HighNullValueRate":[], "GT200_DistinctValue":[]}

    otherMetadata = retrieveOtherMetadata(configMetadata, attributeStats)
    categoricalCols = retrieveCategoricalColumns(configMetadata, features, categoricalCols)
    colnameBucketMetadata, metadataDiagnostics = retrieveColumnBucketMetadata(configMetadata)
    progressReporter.setTotalState(len(colNames))

    index = 1
    dataDiagnostics = []

    for colName in colNames:
        # Update progress
        progressReporter.nextState()
        if colName in features:
            if not otherMetadata.has_key(colName):
                otherMetadata[colName] = (colName, "", "", "")
            if (otherMetadata[colName][1].upper() == "NONE"):
                continue
            index, columnDiagnostics = profileColumn(data[colName], colName, otherMetadata[colName],
                                                     categoricalCols, eventVector, bucketDispatcher, dataWriterFull, index, attributeStats, colnameBucketMetadata.get(colName))
            dataDiagnostics.append(columnDiagnostics)
    writeDiagnostics(dataDiagnostics, metadataDiagnostics, eventVector, features, modelDir, params, attributeStats)
    for v in dataWriterFull.values(): v.close()
    return None

def retrieveOtherMetadata(columnsMetadata, attributeStats):
    qualifiedApprovedUsage = ["None", "Model", "ModelAndModelInsights", "ModelAndAllInsights"]

    otherMetadata = dict()
    if columnsMetadata is None or not columnsMetadata.has_key("Metadata"):
        return otherMetadata
    else:
        columnsMetadata = columnsMetadata["Metadata"]

    for columnMetadata in columnsMetadata:
        colName = columnMetadata['ColumnName']
        try:
            displayName = columnMetadata['DisplayName']
            approvedUsage = columnMetadata['ApprovedUsage']
            extensions = columnMetadata["Extensions"]
            category = ""
            fundamentalType = ""
            if columnMetadata.has_key("FundamentalType"):
                fundamentalType = columnMetadata["FundamentalType"]
            if (extensions is not None and isinstance(extensions, list)):
                for extension in extensions:
                    if extension["Key"] == "Category":
                        category = extension["Value"]

            if displayName is None:
                attributeStats["NULLDisplayName"].append(colName)
                displayName = colName
            if approvedUsage is None:
                approvedUsage = ""
                attributeStats["ApprovedUsage_EmptyOrUnrecognized"].append(colName)
            if isinstance(approvedUsage, list):
                for value in approvedUsage:
                    if value not in qualifiedApprovedUsage: attributeStats["ApprovedUsage_EmptyOrUnrecognized"].append(colName)
                approvedUsage = ",".join(approvedUsage)
            otherMetadata[colName] = (displayName, approvedUsage, category, fundamentalType)
        except:
            logger.warn("Invalid metadata format for column: " + colName)
            continue

    return otherMetadata

def retrieveCategoricalColumns(columnsMetadata, features, categoricalMetadataFromSchema):
    if columnsMetadata is None or not columnsMetadata.has_key("Metadata"):
        return categoricalMetadataFromSchema
    else:
        columnsMetadata = columnsMetadata["Metadata"]
    columnMetadataDict = dict()
    for columnMetadata in columnsMetadata:
        colName = columnMetadata['ColumnName']
        columnMetadataDict[colName] = columnMetadata

    for colName in features:
        if columnMetadataDict.has_key(colName):
            columnMetadata = columnMetadataDict[colName]
            statType = columnMetadata["StatisticalType"] if columnMetadata.has_key("StatisticalType") else None
            if statType is not None:
                if statType == "nominal" or statType == "ordinal":
                    categoricalMetadataFromSchema.add(colName)
                elif (statType == "ratio" or statType == "interval") and colName in categoricalMetadataFromSchema:
                    categoricalMetadataFromSchema.remove(colName)
            else:
                logger.warn("No statistical type for column %s." % colName)

    return categoricalMetadataFromSchema

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
    return sum(map(lambda x: 1 if not isnull(x) else 0, columnData))

def profileColumn(columnData, colName, otherMetadata, stringcols, eventVector, bucketDispatcher, dataWriterFull, index, attributeStats, bucketingParams=None):
    '''
    Performs profiling on given column
    Args:
        columnData: A DataFrame vector of data for given column
        colName: Name of given column
        otherMetadata: Other interesting metadata of a given column
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
    diagnostics["Colname"] = colName
    diagnostics["DisplayName"] = otherMetadata[0]
    filtered = True if colName in attributeStats["ApprovedUsage_EmptyOrUnrecognized"] else False
    if otherMetadata[1] == "Model" and not filtered:
        attributeStats["ApprovedUsage_Model"].append(colName)
        filtered = True
    if (isnull(otherMetadata[2]) or otherMetadata[2] == "") and not filtered:
        attributeStats["NULLCategory"].append(colName)
        filtered = True
    diagnostics["PopulationRate"] = getPopulatedRowCount(columnData, colName not in stringcols) / float(len(columnData))
    if diagnostics["PopulationRate"] < 0.005 and not filtered:
        attributeStats["HighNullValueRate"].append(colName)
        filtered = True

    if diagnostics["PopulationRate"] == 0.0:
        return (index, diagnostics)

    logger.info("Processing column %s." % colName)
    if colName in stringcols:
        # Categorical column
        columnData = columnData.apply(lambda col: None if isnull(col) else str(col))
        diagnostics["Type"] = "Categorical"
        uniqueValues = len(columnData.unique())
        mode = columnData.value_counts().idxmax()
        diagnostics["UniqueValues"] = uniqueValues
        groupingDict = getCatGroupingAndStatsForModel(columnData.tolist(), eventVector.tolist())
        index, diagnostics["UncertaintyCoefficient"] = writeCategoricalValuesToAvro(dataWriterFull['Model'], groupingDict, mode, colName, otherMetadata, index)
        groupingDict = getCatGroupingAndStatsForDisplay(groupingDict['groupingResults'])
        if uniqueValues > 200:
            if not filtered: attributeStats["GT200_DistinctValue"].append(colName)
            groupingDict['groupingResults']['LATTICE_GT200_DistinctValue'] = (0, 0.0, 0.0)
        index, diagnostics["UncertaintyCoefficient"] = writeCategoricalValuesToAvro(dataWriterFull['UI'], groupingDict, mode, colName, otherMetadata, index)
    else:
        # Band column
        diagnostics["Type"] = "Band"
        # Convert all continuous values into a numeric data type
        if columnData.dtype == np.object_:
            columnData = pd.Series(pd.lib.maybe_convert_numeric(columnData.as_matrix(), set(), coerce_numeric=True))
        mean = columnData.mean()
        median = columnData.median()
        skewness, kurtosis = getKurtosisAndSkewness(columnData)
        # do we still need this?
        if math.isnan(median):
            logger.warn("Median to impute for column name: " + colName + " is null; excluding this column.")
            return (index, diagnostics)
        # use only unified bucketer and return different things
        # Apply bucketing with specified parameters for MI
        bandsDictMI = getBucketsAndStats(columnData.tolist(), eventVector,  rawBinNumber = 200,
                       minPtsToBucket = 50, minSpecialValRatio = 0.02, minRegBucketSize = 20,
                       minRegBucketRatio = 0.005, sigThreshold = 8, minNumBuckets = 3, 
                       maxNumBuckets = 10, forUI = False, kurtThreshold=3, numTailBuckets=3, 
                       tailSize=0.03, sizeThreshold=0.05, liftThreshold=1.8, 
                       fixBoundaries=False, eventDiffThreshold=10)
        # use different parameters for UI buckets
        # if colname is in revenue or employees, fix boundaries
        fixBoundCols = ["BusinessEstimatedAnnualSales", "BusinessEstimatedEmployees",
                        "EMPLOYEES_HERE", "EMPLOYEES_TOTAL", "LE_EMPLOYEE_RANGE",
                        "LE_NUMBER_OF_LOCATIONS", "NUMBER_OF_FAMILY_MEMBERS",
                        "SALES_VOLUME_LOCAL_CURRENCY", "SALES_VOLUME_US_DOLLARS",
                        "Total_Amount_Raised"]
        fixBoundCols = [x.lower() for x in fixBoundCols]
        fixbound = colName.lower() in fixBoundCols
        bandsDictUI = getBucketsAndStats(columnData.tolist(), eventVector,  rawBinNumber = 100,
                       minPtsToBucket = 50, minSpecialValRatio = 0.02, minRegBucketSize = 100,
                       minRegBucketRatio = 0.02, sigThreshold = 8, minNumBuckets = 3, 
                       maxNumBuckets = 7, forUI = True, kurtThreshold=3, numTailBuckets=3, 
                       tailSize=0.03, sizeThreshold=0.05, liftThreshold=1.8, 
                       fixBoundaries=fixbound, eventDiffThreshold=10)
        diagnostics["BucketingStrategy"] = "UnifiedBucketer"
        index, diagnostics["UncertaintyCoefficient"] = writeBandsToAvro(dataWriterFull['Model'], bandsDictMI,
                                                        mean, median, kurtosis, skewness, colName, otherMetadata, index)
        index, diagnostics["UncertaintyCoefficient"] = writeBandsToAvro(dataWriterFull['UI'], bandsDictUI,
                                                        mean, median, kurtosis, skewness, colName, otherMetadata, index)

    return (index, diagnostics)

def getKurtosisAndSkewness(columnData):
    try:
        _, (_, _), _, _, skewness, kurtosis = stats.describe(columnData.data)
        return skewness, kurtosis
    except ValueError:
        logger.warn("Skewness and Kurtosis could not be calculated because of ValueError thrown.")
        return None, None
    except IndexError:
        logger.warn("Skewness and Kurtosis could not be calculated because of IndexError thrown.")
        return None, None
    except Exception:
        return None, None


def writeCategoricalValuesToAvro(dataWriter, groupingDict, mode, colName, otherMetadata, index):
    '''
    Creates a datum for each unique value in the categorical column and writes to buffered writer
    Args:
        dataWriter: Buffered writer which appends each datum to the avro file
        groupingDict: results after categorical grouping
        mode: Mode of all values in the column vector
        colName: Name of given column
        index: Current id of column in output file
    Returns:
        index: id of next column in output file
    '''
    groupingResults = groupingDict['groupingResults']
    mi = groupingDict['mi']
    entropyValue = groupingDict['entropyValue']
    uncertaintyCoeffDict = groupingDict['uncertaintyCoefficient']
    
    for key, value in groupingResults.items():
        datum = {}
        datum["id"] = index
        datum["barecolumnname"] = colName
        datum["displayname"] = otherMetadata[0]
        datum["approvedusage"] = otherMetadata[1]
        datum["category"] = otherMetadata[2]
        datum["fundamentaltype"] = otherMetadata[3]
        datum["columnvalue"] = key
        datum["Dtype"] = "STR"
        datum["minV"] = None
        datum["maxV"] = None
        datum["mean"] = None
        datum["median"] = None
        datum["mode"] = mode
        datum["kurtosis"] = None
        datum["skewness"] = None
        datum["count"] = value[0]
        datum["lift"] = value[2]
        datum["uncertaintyCoefficient"] = uncertaintyCoeffDict[key] if key in uncertaintyCoeffDict else None
        datum["discreteNullBucket"] = key is None
        datum["continuousNullBucket"] = False
        datum["positiveEventCount"] = int(value[1])
        index = index + 1
        dataWriter.append(datum)

    return index, uncertaintyCoefficient(mi, entropyValue)

def writeBandsToAvro(dataWriter, bandsDict, mean, median, kurtosis, skewness, colName, otherMetadata, index):
    '''
    Creates a datum for each band in the band column and writes to buffered writer
    Args:
        dataWriter: Buffered writer which appends each datum to the avro file
        bandsDict: A dictionary of bands and related information
        colName: Name of given column
        index: Current id of column in output file
    Returns:
        index: id of next column in output file
    '''
    
    for idx in range(len(bandsDict["min"])):
        datum = {}
        datum["id"] = index
        datum["barecolumnname"] = colName
        datum["displayname"] = otherMetadata[0]
        datum["approvedusage"] = otherMetadata[1]
        datum["category"] = otherMetadata[2]
        datum["fundamentaltype"] = otherMetadata[3]
        datum["columnvalue"] = None
        datum["Dtype"] = "BND"
        datum["minV"] = bandsDict["min"][idx]
        datum["maxV"] = bandsDict["max"][idx]
        datum["mean"] = mean
        datum["median"] = median
        datum["mode"] = None
        datum["kurtosis"] = kurtosis
        datum["skewness"] = skewness
        datum["count"] = bandsDict["sampleCount"][idx]
        datum["lift"] = bandsDict["lift"][idx]
        datum["uncertaintyCoefficient"] = bandsDict["uncertaintyCoefficient"][idx]
        datum["discreteNullBucket"] = False
        datum["continuousNullBucket"] = datum["minV"] is None and datum["maxV"] is None
        datum["positiveEventCount"] = int(bandsDict["eventCount"][idx])
        index = index + 1
        dataWriter.append(datum)
    return index, uncertaintyCoefficient(bandsDict["mi"], bandsDict["entropyValue"])

def writeDiagnostics(dataDiagnostics, metadataDiagnostics, eventVector, features, modelDir, params, attributeStats):
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
    summary = getSummaryDiagnostics(dataDiagnostics, eventVector, features, params, attributeStats)

    diagnostics = OrderedDict()
    diagnostics["Summary"] = summary
    diagnostics["MetadataDiagnostics"] = metadataDiagnostics
    diagnostics["ColumnDiagnostics"] = dataDiagnostics
    diagnostics["Version"] = "2.0"

    with open(modelDir + "diagnostics.json", "wb") as fp:
        json.dump(diagnostics, fp)

def getSummaryDiagnostics(dataDiagnostics, eventVector, features, params, attributeStats):
    summary = OrderedDict()
    summary["SampleSize"] = len(eventVector)
    summary["ColumnSize"] = len(features)
    summary["PositiveEventRate"] = sum(eventVector) / float(len(eventVector))
    summary.update(attributeStats)
    highUCThreshold = 0.2
    if params is not None:
        parser = params["parser"]
        summary["NumberOfSkippedRows"] = parser.numOfSkippedRow

        highUCThreshold = parser.highUCThreshold

    highUCColumns = []
    for columnDiagnostics in dataDiagnostics:
        if columnDiagnostics.has_key("UncertaintyCoefficient") and columnDiagnostics["UncertaintyCoefficient"] > highUCThreshold:
            highUCColumns.append(columnDiagnostics['Colname'])
    if len(highUCColumns) > 0:
        summary["HighUCColumns"] = ",".join(highUCColumns)
    return summary

def uncertaintyCoefficient(mi, entropyVal):
    if mi == None or entropyVal == 0:
        return None
    return mi / entropyVal