import math
import csv
from itertools import groupby
import pandas as pd

# given the length and conversion rate of sub-population and the length and conversion rate of overall-population, calculate the significance
def sigCalculation(sub, overall):
    subCnt, subRate = sub
    oaCnt, oaRate = overall
    r = (subCnt * subRate + oaCnt * oaRate) / (subCnt + oaCnt)
    return (subRate - oaRate) / (math.sqrt(r * (1.0 - r) * (1.0 / subCnt + 1.0 / oaCnt)))

# given an array of events, calculate the conversion rate
def getRate(eventCol):
    return sum(eventCol) * 1.0 / len(eventCol)

# given the length of data points and desirable number of buckets, return how many data points should be in each bucket
def bucketing(lenData, numBucket):
    numPtinBucket = int(lenData / float(numBucket))
    if numPtinBucket == 0:
        return [0, lenData]
    leftover = lenData - numPtinBucket * numBucket
    cutPosition = [(numPtinBucket + 1) * i if i <= leftover else numPtinBucket * i + leftover for i in range(numBucket + 1)]
    return cutPosition


def discretizeNumericVar(inputCol, numBucket=20, emptyValList=[None, "Null", "Empty", ""]):

    idxNonEmpty = [i for i, x in enumerate(inputCol) if x not in emptyValList and not math.isnan(x)]

    if len(idxNonEmpty) == 0:
        return [range(len(inputCol))]

    idxEmpty = [i for i, x in enumerate(inputCol) if x in emptyValList or math.isnan(x)]

    inputColNonEmpty = [inputCol[i] for i in idxNonEmpty]

    sortedInd = [y[0] for y in sorted(enumerate(inputColNonEmpty), key=lambda x: x[1])]

    idxNonEmptySorted = [idxNonEmpty[i] for i in sortedInd]

    idxCut = bucketing(len(idxNonEmpty), numBucket)

    return [idxEmpty] + [[idxNonEmptySorted[i] for i in range(idxCut[bucketIdx], idxCut[bucketIdx + 1])] for bucketIdx in range(len(idxCut) - 1)]


# given a column of numerical data, discretize the data into categorical column. The null value are all in bucket 0, the non-null values are put in buckets ranging from 1 to numBucket
def numricVarToCategVar(inputCol, numBucket=20, emptyValList=[None, "Null", "Empty", ""]):

    bucketIdxList = discretizeNumericVar(inputCol, numBucket, emptyValList)

    categIdx = []
    idxList = []
    for i in range(len(bucketIdxList)):
        if len(bucketIdxList[i]) > 0:
            idxList = idxList + bucketIdxList[i]
            categIdx = categIdx + [i] * len(bucketIdxList[i])

    sortedInd = [y[0] for y in sorted(enumerate(idxList), key=lambda x: x[1])]

    return [categIdx[i] for i in sortedInd]

# get the column of feature value. if it's a numerical column, discretize it into buckets
def getColVal(colVal, colType, numBucket):
    if colType == 'cat':
        return colVal
    elif colType == 'num':
        return numricVarToCategVar(colVal, numBucket)

# get the conversion rate for each feature value
def getGroupedRate(colVal, eventCol, cntRate_overall=None):

    dataGrouped = groupby(sorted(zip(colVal, eventCol), key=lambda y: y[0]), lambda z: z[0])

    dataGrouped = {key : [x for x in group] for key, group in dataGrouped}

    cntRateGrouped = {key : (len(group), getRate([x[1] for x in group])) for (key, group) in dataGrouped.items()}

    if cntRate_overall is None:
        cntRateGrouped = {key : (x[0], x[1]) for (key, x) in cntRateGrouped.items()}
    else:
        cntRateGrouped = {key : (x[0], x[1], sigCalculation(x, cntRate_overall)) for (key, x) in cntRateGrouped.items()}
    return cntRateGrouped

def strValueFix(x):
    nullWordsPart = set(['missing', 'available', 'empty', 'bogus'])
    nullWordsFull = set(['nan', 'null'])
    y = str(x)
    if pd.isnull(x) or any(z in y.lower() for z in nullWordsPart) or any(z == y.lower() for z in nullWordsFull) :  return ''
    return y

def convertCleanDataFrame(colNames, df, catColumnNamesSet, numColumnNamesSet, returnListofCols=True):
    allCols = []

    for c in colNames:
        if c in catColumnNamesSet:
            colType = 'cat'
        if c in numColumnNamesSet:
            colType = 'num'
        col = df[c].tolist()
        if colType == 'cat':
            col = [strValueFix(x) for x in col]
        if colType == 'num':
            try:
                col = [x if pd.isnull(x) else float(x) for x in col]
            except Exception as e:
                print "Exception while converting to float: %s" % e
                print c, "is columnName that caused an exception"
                print "Column values and types"
                for x in col:
                    print x, type(x)
        allCols.append(col)
    if returnListofCols: return allCols
    numRows = len(allCols[0])
    allRows = [[allCols[j][i] for j in range(len(allCols))] for i in range(numRows)]
    return allRows

def selectIdColumn(dataFrame):
    if "Id" in dataFrame.columns:
        return "Id"
    elif "LeadID" in dataFrame.columns:
        return "LeadID"
    elif "ExternalID" in dataFrame.columns:
        return "ExternalID"
