from collections import defaultdict

import encoder
import numpy as np
import pandas as pd
from pipelinefwk import PipelineStep
from pipelinefwk import get_logger
import random as rd


logger = get_logger("pipeline")
 
def dictfreq(doc):
    freq = defaultdict(int)
    freq[doc] += 1
    return freq
 
class EnumeratedColumnTransformStep(PipelineStep):
    enumMappings_ = {}
 
    def __init__(self, enumMappings):
        self.enumMappings_ = enumMappings
         
    def transform(self, dataFrame):
        outputFrame = dataFrame
 
        for column, encoder in self.enumMappings_.iteritems():
            if hasattr(encoder, 'classes_'):
                classSet = set(encoder.classes_.flat)
                outputFrame[column] = outputFrame[column].map(lambda s: 'NULL' if s not in classSet else s)
                encoder.classes_ = np.append(encoder.classes_, 'NULL')
             
            if column in outputFrame:
                logger.info("Transforming column %s." % column)
                outputFrame[column] = encoder.transform(outputFrame[column])
 
        return outputFrame
 
class ColumnTypeConversionStep(PipelineStep):
    columnsToConvert_ = []
     
    def __init__(self, columnsToConvert=[]):
        self.columnsToConvert_ = columnsToConvert
 
    def transform(self, dataFrame):
        if len(self.columnsToConvert_) > 0:
            for column in self.columnsToConvert_:
                if column in dataFrame and dataFrame[column].dtype == np.object_:
                    dataFrame[column] = pd.Series(pd.lib.maybe_convert_numeric(dataFrame[column].as_matrix(), set(), coerce_numeric=True))
                else:
                    logger.info("Column %s cannot be transformed since it is not in the data frame." % column)
        return dataFrame
     
class ImputationStep(PipelineStep):
    enumMappings_ = dict()
    imputationValues_ = {}
    targetColumn_ = ""
     
    def __init__(self, enumMappings, imputationValues, targetCol):
        self.enumMappings_ = enumMappings
        self.imputationValues_ = imputationValues
        self.targetColumn_ = targetCol
 
    def transform(self, dataFrame):
        calculateImputationValues = True
        if len(self.imputationValues_) != 0:
            calculateImputationValues = False
               
        outputFrame, nullValues = self.generateIsNullColumns(dataFrame)
        if len(self.enumMappings_) > 0:
            if self.targetColumn_ in outputFrame and calculateImputationValues == True:
                expectedLabelValue = self.getExpectedLabelValue(outputFrame, self.targetColumn_) 
             
            for column, value in self.enumMappings_.iteritems():
                if column in outputFrame:
                    if nullValues[column] > 0 and calculateImputationValues == True:
                        imputationBins = self.createBins(outputFrame[column], outputFrame[self.targetColumn_], 20)
                        imputationValue = self.matchValue(expectedLabelValue, imputationBins)
                        self.imputationValues_[column] = imputationValue
 
                    try:
                        outputFrame[column] = outputFrame[column].fillna(self.imputationValues_[column])
                    except KeyError:
                        self.imputationValues_[column] = value
                        outputFrame[column] = outputFrame[column].fillna(self.imputationValues_[column])
 
        return outputFrame
     
    def getExpectedLabelValue(self, dataFrame, targetCol):
        zeroLabels = (dataFrame[targetCol] == 0).sum()
        oneLabels = (dataFrame[targetCol] == 1).sum()
        expectedLabelValue = float(oneLabels) / (oneLabels + zeroLabels)
        return expectedLabelValue
         
    def generateIsNullColumns(self, dataFrame):
        nullValues = {}
        outputFrame = dataFrame
        if len(self.enumMappings_) > 0:
            for column, _ in self.enumMappings_.iteritems():
                if column in outputFrame:
                    _, nullCount = self.getIsNullColumn(outputFrame[column])
                    if nullCount > 0:
                        nullValues[column] = nullCount
                    else:
                        nullValues[column] = 0
        return outputFrame, nullValues
             
    def getIsNullColumn(self, dataColumn):
        nullCount = 0
        isNullColumn = []
        for i in range(len(dataColumn)):
            if pd.isnull(dataColumn[i]):
                nullCount = nullCount + 1
                isNullColumn.append(1)
            else:
                isNullColumn.append(0)
        return isNullColumn, nullCount
     
    def createIndexSequence(self, number, rawSplits):
        if number == 0:
            return []
          
        binSize = int(number / (rawSplits + 1) / 1.0)
        numBins = int(number / binSize)
        sp = [binSize * i for i in range(numBins + 1)]
        sp[numBins] = number
        return tuple(sp)
     
    def meanValuePair(self, x,tupleSize=2):
        def mean(k):  return sum([float(y[k]) for y in x]) / len(x) / 1.0
        if tupleSize > 1: return [mean(k) for k in range(tupleSize)]
        return [sum(x) / 1.0 / len(x)]
 
    def createBins(self, x, y, numBins):
        if len(x) != len(y):
            logger.warn("Warning: Number of records and number of labels are different.")
 
        pairs = []
        numberOfPoints = min(len(x), len(y))
        for i in range(numberOfPoints):
            if pd.isnull(x[i]) == False:
                pairs.append([x[i], y[i]])
         
        if numBins > len(pairs) - 1 and len(pairs) != 0:
            numBins = len(pairs)- 1
 
        indBins = self.createIndexSequence(len(pairs), numBins)
        def mvPair(k):
            return self.meanValuePair([pairs[i] for i in range(indBins[k], indBins[k+1])])
        return pd.Series([mvPair(b) for b in range(len(indBins)-1)])
 
    def matchValue(self, yvalue, binPairs):
        def absFn(x): return( x if x > 0 else (-x))
        def sgnFn(x):
            if x == 0:
                return x
            else:
                return (1 if x > 0 else (-1))
 
        numBins = len(binPairs)
         
        if numBins == 1:
            return binPairs[0][0]
        elif numBins == 0:
            return rd.randint(10, 1000)
         
        matches = [absFn(yvalue - p[1]) for p in binPairs]
        ind = [i for i,x in enumerate(matches) if x == min(matches)][0]
        valuePair = binPairs[ind]
         
        splitValue = binPairs[1][0] - binPairs[0][0]
        adjValue=0
        if valuePair == binPairs[0]:
            adjValue = -2.0 * splitValue
        if valuePair == binPairs[numBins - 1]:
            adjValue = 2.0 * splitValue
        return valuePair[0] + adjValue 