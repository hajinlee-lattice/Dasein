from avro import schema, datafile, io
import codecs
import re
import sys

from leframework.executors.dataprofilingexecutor import DataProfilingExecutor
import pandas as pd
import pandas.core.algorithms as algos
import numpy as np
import argparse
import sklearn.metrics
from sklearn.metrics.cluster.supervised import contingency_matrix, check_clusterings



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
    dataWriter = datafile.DataFileWriter(codecs.open(modelDir + '/metadata.avro', 'wb'), recordWriter, writers_schema=avroSchema, codec='deflate')
    
    colnames = list(data.columns.values)
    stringcols = set(schema["stringColumns"])
    features = set(schema["features"])
    index = 1
    event = set(schema["targets"])
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
                bands = binContinuousColumn(data[colname], 5, event)
                index = writeBandsToAvro(dataWriter, bands, mean, median, colname, index)
            except Exception:
                continue
    dataWriter.close()
    return None

def calculateMI(labels_true, labels_pred):
    labels_true, labels_pred = check_clusterings(labels_true, labels_pred)
    
    # pick a small enough epsilon so zero entries won't impace MI calc
    contingency = contingency_matrix(labels_true, labels_pred, 0.0000001)
    contingency = np.array(contingency, dtype='float')
    contingency_sum = np.sum(contingency)
    pi = np.sum(contingency, axis=1)
    pj = np.sum(contingency, axis=0)
    outer = np.outer(pi, pj)
   
    outer = outer.flatten()
    oldShape = np.shape(contingency)
    contingency_nm = contingency.flatten()

    log_contingency_nm = np.log(contingency_nm)
    contingency_nm /= contingency_sum
    # log(a / b) should be calculated as log(a) - log(b) for
    # possible loss of precision
    log_outer = -np.log(outer) + np.log(pi.sum()) + np.log(pj.sum())
    mi = (contingency_nm * (log_contingency_nm - np.log(contingency_sum))
          + contingency_nm * log_outer)
          
    mi = np.reshape(mi, oldShape)
    return mi
    

def calculateBucketStatistics(binnedColumnSeries, eventSeries):
    miComponents = calculateMI(eventSeries, binnedColumnSeries)
    miFrame = pd.DataFrame(data=miComponents)
    miSeries = miFrame.sum()
    
    statsFrame = pd.concat([binnedColumnSeries, eventSeries], axis=1)
    avgProb = statsFrame['Event'].mean()
    statsGrouping = statsFrame.groupby(binnedColumnSeries.name)
    observedProb = statsGrouping['Event'].mean()
    lift = observedProb / avgProb    
    lift.name = 'Lift'    
    
    outputFrame = pd.DataFrame(lift)
    outputFrame['MI'] = miSeries
    outputFrame['Bin Size'] = statsGrouping['Bin'].count()
    outputFrame['Event Count'] = statsGrouping['Event'].sum()    
    
    return outputFrame

def binContinuousColumn(columnSeries, numbins, eventSeries):
    populatedRows = columnSeries[columnSeries.notnull()]
    ranges = np.linspace(0, 1, numbins + 1, endpoint=True)
    betterBins = algos.quantile(populatedRows, ranges)
    
    # combine adjacent buckets of equal value and try to break up the remaining range evenly
    for i in range(numbins):
        if betterBins[i] == betterBins[i + 1]:
            remainingRanges = np.linspace(0, 1, numbins - i)
            betterBins[i + 1:] = algos.quantile(populatedRows[populatedRows > betterBins[i]], remainingRanges)
    
    betterBins[0] = -np.inf
    betterBins[-1] = np.inf
    betterBins = betterBins[~np.isnan(betterBins)]
    
    binnedFrame = pd.DataFrame(columnSeries)
    
    binnedFrame['Bin'] = np.nan
    binnedFrame.ix[binnedFrame[columnSeries.name].isnull(), ['Bin']] = 0
    
    for i in range(len(betterBins) - 1):
        binnedFrame.ix[(binnedFrame[columnSeries.name] >= betterBins[i]) & (binnedFrame[columnSeries.name] < betterBins[i + 1]), ['Bin']] = i + 1   
    
    binStats = calculateBucketStatistics(binnedFrame['Bin'], eventSeries)
    # print binStats
    
    dictList = []
    
    if (binnedFrame['Bin'] == 0).any():
        nullAttributes = {
            'ColumnName': columnSeries.name,
            'Band Description':'Unknown',
            'Lower Inclusive':'NULL',
            'Upper Exclusive':'NULL',
            'Lift':binStats.iloc[0]['Lift'],
            'Mutual Information':binStats.iloc[0]['MI'],
            'Bin Size':binStats.iloc[0]['Bin Size'],
            'Event Count':binStats.iloc[0]['Event Count']}
        dictList.append(nullAttributes)
    
    # print columnSeries.name
    for i in range(len(betterBins) - 1):
        # handle edge cases where algos.quantile had to interpolate and there were no entries for that bin
        # XXX Jake: this might be better handled by setting the interpolation to 'lowest' or 'highest' in the quantile call
        if (i + 1) in binStats.index:
            attributeStats = {
                'ColumnName': columnSeries.name,
                'Band Description': ("{0} <= " + columnSeries.name + " < {1}").format(betterBins[i], betterBins[i + 1]),
                'Lower Inclusive':str(betterBins[i]),  # if betterBins[i] != -np.inf else "'-inf'",
                'Upper Exclusive':str(betterBins[i + 1]),
                'Lift':binStats.ix[i + 1]['Lift'],
                'Mutual Information':binStats.ix[i + 1]['MI'],
                'Bin Size': binStats.ix[i + 1]['Bin Size'],
                'Event Count':binStats.ix[i + 1]['Event Count']}
                
            dictList.append(attributeStats)
        
    outputFrame = pd.DataFrame(dictList)
    return outputFrame

def writeCategoricalValuesToAvro(dataWriter, uniquevalues, mode, colname, index):
    if len(uniquevalues) > 200:
        return index
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
        index = index + 1
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
        index = index + 1
        dataWriter.append(datum)
    
    return index

    
