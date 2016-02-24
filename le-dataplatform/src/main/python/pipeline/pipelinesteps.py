from collections import OrderedDict

import encoder
import numpy as np
import os
import pandas as pd
from pipelinefwk import PipelineStep
from pipelinefwk import create_column
from pipelinefwk import get_logger
import random as rd


logger = get_logger("pipeline")
    
class EnumeratedColumnTransformStep(PipelineStep):
    columns = {}
    columnList = []
    
    def __init__(self, columns):
        self.columns = columns
        self.columnList = columns.keys()
            
    def transform(self, dataFrame, configMetadata, test):
        outputFrame = dataFrame
    
        for column, encoder in self.columns.iteritems():
            if hasattr(encoder, 'classes_'):
                classSet = set(encoder.classes_.flat)
                outputFrame[column] = outputFrame[column].map(lambda s: 'NULL' if s not in classSet else s)
                encoder.classes_ = np.append(encoder.classes_, 'NULL')
                
            if column in outputFrame:
                logger.info("Transforming column %s." % column)
                outputFrame[column] = encoder.transform(outputFrame[column])
    
        return outputFrame
    
    def getOutputColumns(self):
        return [(create_column(k, "LONG"), [k]) for k in self.columnList]

    def getRTSMainModule(self):
        return "encoder"
    
class ColumnTypeConversionStep(PipelineStep):
    columnsToPivot = []
        
    def __init__(self, columnsToPivot=[]):
        self.columnsToPivot = columnsToPivot
    
    def transform(self, dataFrame, configMetadata, test):
        if len(self.columnsToPivot) > 0:
            for column in self.columnsToPivot:
                if column in dataFrame and dataFrame[column].dtype == np.object_:
                    dataFrame[column] = pd.Series(pd.lib.maybe_convert_numeric(dataFrame[column].as_matrix(), set(), coerce_numeric=True))
                else:
                    logger.info("Column %s cannot be transformed since it is not in the data frame." % column)
        return dataFrame
    
    def getOutputColumns(self):
        return [(create_column(k, "FLOAT"), [k]) for k in self.columnsToPivot]
    
    def getRTSMainModule(self):
        return "make_float"

        
class ImputationStep(PipelineStep):
    columns = OrderedDict()
    imputationValues = {}
    targetColumn = ""
    imputationFilePath = None

    def __init__(self, columns, imputationValues, targetCol):
        self.columns = columns
        self.imputationValues = imputationValues
        self.targetColumn = targetCol

    def getOutputColumns(self):
        return [(create_column(k, "FLOAT"), [k]) for k, _ in self.columns.iteritems()]

    def getRTSMainModule(self):
        return "replace_null_value"

    def getRTSArtifacts(self):
        return [("imputations.txt", self.imputationFilePath)]

    def transform(self, dataFrame, configMetadata, test):
        if len(self.imputationValues) == 0:
            self.imputationValues = self.__computeImputationValues(dataFrame)
            self.__writeRTSArtifact()
        for column, value in self.columns.iteritems():
            if column in dataFrame:
                try:
                    dataFrame[column] = dataFrame[column].fillna(self.imputationValues[column])
                except KeyError:
                    dataFrame[column] = dataFrame[column].fillna(value)
        return dataFrame
    
    def __writeRTSArtifact(self):
        with open("imputations.txt", "w") as fp:
            fp.write(str(self.imputationValues))
            self.imputationFilePath = os.path.abspath(fp.name)
  
    def __getNullValues(self, dataFrame):
        nullValues = {}
        outputFrame = dataFrame

        if len(self.columns) > 0:
            for column in self.columns:
                if column in outputFrame:
                    nullCount = self.__getIsNullColumn(outputFrame[column])
                    nullValues[column] = nullCount
        return nullValues
                 
    def __getIsNullColumn(self, dataColumn):
        nullCount = 0
        
        for i in range(len(dataColumn)):
            if pd.isnull(dataColumn[i]):
                nullCount = nullCount + 1
        return nullCount
        
    def __computeImputationValues(self, dataFrame):
        nullValues = self.__getNullValues(dataFrame)
        outputFrame = dataFrame
        imputationValues = {}
        if len(self.columns) > 0:
            expectedLabelValue = 0.0
            if self.targetColumn in outputFrame:
                expectedLabelValue = self.__getExpectedLabelValue(outputFrame, self.targetColumn)
                 
            for column, value in self.columns.iteritems():
                if column in outputFrame:
                    try:
                        if nullValues[column] > 0:
                            imputationBins = self.__createBins(outputFrame[column], outputFrame[self.targetColumn])
                            imputationValue = self.__matchValue(expectedLabelValue, imputationBins)
                            imputationValues[column] = imputationValue
                    except KeyError:
                        imputationValues[column] = value
        return imputationValues
        
    def __getExpectedLabelValue(self, dataFrame, targetColumn):
        zeroLabels = (dataFrame[targetColumn] == 0).sum()
        oneLabels = (dataFrame[targetColumn] == 1).sum()
        expectedLabelValue = float(oneLabels) / (oneLabels + zeroLabels)
        return expectedLabelValue
        
    def __createIndexSequence(self, number, rawSplits):
        if number == 0:
            return []
              
        binSize = int(number / (rawSplits + 1) / 1.0)
        numBins = int(number / binSize)
        sp = [binSize * i for i in range(numBins + 1)]
        sp[numBins] = number
        return tuple(sp)
         
    def __meanValuePair(self, x, tupleSize=2):
        def mean(k):  return sum([float(y[k]) for y in x]) / len(x) / 1.0
        if tupleSize > 1: return [mean(k) for k in range(tupleSize)]
        return [sum(x) / 1.0 / len(x)]
     
    def __createBins(self, x, y, numBins=20):
        if len(x) != len(y):
            print "Warning: Number of records and number of labels are different."
     
        pairs = []
        numberOfPoints = min(len(x), len(y))
        for i in range(numberOfPoints):
            if pd.isnull(x[i]) == False:
                pairs.append([x[i], y[i]])
             
        if numBins > len(pairs) -1 and len(pairs) != 0:
            numBins = len(pairs)-1
     
        indBins = self.__createIndexSequence(len(pairs), numBins)
        def mvPair(k):
            return self.__meanValuePair([pairs[i] for i in range(indBins[k], indBins[k+1])])
        return pd.Series([mvPair(b) for b in range(len(indBins)-1)])
     
    def __matchValue(self, yvalue, binPairs):
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