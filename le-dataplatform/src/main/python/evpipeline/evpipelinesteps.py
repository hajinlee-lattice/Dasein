from collections import OrderedDict
import re

from sklearn.decomposition import PCA

import encoder
import numpy as np
import pandas as pd
from pipelinefwk import PipelineStep
from pipelinefwk import get_logger
import random as rd


logger = get_logger("evpipeline")
    
class EnumeratedColumnTransformStep(PipelineStep):
    columns = {}
    
    def __init__(self, enumMappings):
        self.columns = enumMappings
            
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
    
class ColumnTypeConversionStep(PipelineStep):
    columnsToConvert_ = []
        
    def __init__(self, columnsToPivot=[]):
        self.columnsToConvert_ = columnsToPivot
    
    def transform(self, dataFrame, configMetadata, test):
        if len(self.columnsToConvert_) > 0:
            for column in self.columnsToConvert_:
                if column in dataFrame and dataFrame[column].dtype == np.object_:
                    dataFrame[column] = pd.Series(pd.lib.maybe_convert_numeric(dataFrame[column].as_matrix(), set(), coerce_numeric=True))
                else:
                    logger.warn("Column %s cannot be transformed since it is not in the data frame." % column)
        return dataFrame
        
class ImputationStep(PipelineStep):
    columns = OrderedDict()
    columnsMax = OrderedDict()
    imputationValues = {}
    targetColumn = ""    
    scalingArray = []
    meanColumn = []
    componentMatrix = []
      
    def __init__(self, enumMappings, imputationValues, scalingArray, meanColumn, columnsMax, componentMatrix, targetCol):
        self.columns = enumMappings
        self.imputationValues = imputationValues
        self.targetColumn = targetCol
        self.scalingArray = scalingArray
        self.meanColumn = meanColumn
        self.componentMatrix = componentMatrix
        self.columnsMax = columnsMax
     
    def transform(self, dataFrame, configMetadata, test):
        calculateImputationValues = True
        if len(self.imputationValues) != 0:
            calculateImputationValues = False

        outputFrame, nullValues = self.generateTransformedBooleanColumns(dataFrame, calculateImputationValues)
        outputFrame = self.__computeImputationValues(outputFrame, nullValues, calculateImputationValues)
          
        return outputFrame
   
    def generateTransformedBooleanColumns(self, dataFrame, calculateImputationValues):
        outputFrame = dataFrame
        nullCols, nullValues = self.generateIsNullColumns(outputFrame)

        if len(nullValues) > 0:
            if calculateImputationValues:
                (self.scalingArray, self.meanColumn, self.componentMatrix) = self.nullValuePCA(nullCols, outputFrame[self.targetColumn])

            nullCol_transformed = self.getTransformedMatrix(self.scalingArray, self.meanColumn, self.componentMatrix, nullCols)
            if len(nullCol_transformed > 0):
                nullCol_transformed = pd.DataFrame(nullCol_transformed)
                nullCol_transformed.columns = ["Transformed_Boolean_" + str(nullCol_transformed.columns.values[i]) for i in range(len(nullCol_transformed.columns.values))]
                outputFrame = pd.concat([outputFrame, nullCol_transformed], axis=1)
        return outputFrame, nullValues
        
    def generateIsNullColumns(self, dataFrame):
        nullValues = {}
        outputFrame = dataFrame
        nullColsFrame = pd.DataFrame()
     
        if len(self.columns) > 0:
            for column in self.columns:
                if column in outputFrame:
                    isNullColumn, nullCount = self.__getIsNullColumn(outputFrame[column])
                    nullValues[column] = nullCount
                    nullColsFrame[column + "_isNull"] = pd.Series(isNullColumn)

        return nullColsFrame, nullValues
                 
    def __getIsNullColumn(self, dataColumn):
        nullCount = 0
        isNullColumn = []
        for i in range(len(dataColumn)):
            if pd.isnull(dataColumn[i]):
                nullCount = nullCount + 1
                isNullColumn.append(1.0)
            else:
                isNullColumn.append(0.0)
        return isNullColumn, nullCount
        
    def getScalingForPCA(self, dataFrame, eventColumn):
        ratePopulation = np.mean(eventColumn)
        scalarList = []
        for col in dataFrame.columns:
            rateColumn = np.mean(eventColumn[dataFrame[col] > 0])
            if np.isnan(rateColumn):
                rateColumn = 0
                     
            scalarList.append(rateColumn - ratePopulation)
        return np.array(scalarList)
         
    def getindexofMaxVariance(self, explainedVarianceRatio, thresholdVariance):
        sumVarianceRatio = 0
        for i in range(len(explainedVarianceRatio)):
            sumVarianceRatio += explainedVarianceRatio[i]
            if sumVarianceRatio > thresholdVariance:
                return i + 1
         
    def getPCAComponents(self, X):
        pca = PCA()
        pca.fit(X)
        explainedVarianceRatio = pca.explained_variance_ratio_
        componentsMatrix = pca.components_
        X_transformed = pca.transform(X)
             
        return (explainedVarianceRatio, componentsMatrix, X_transformed)  
        
    def nullValuePCA(self, inputDF, eventCol, thresholdVariance=0.98, numberOfColumnsThreshold=5):
        if len(inputDF.columns.values) < numberOfColumnsThreshold:
            numberOfColumnsThreshold = len(inputDF.columns.values) - 1
  
        scaling_array = self.getScalingForPCA(inputDF, eventCol)
        inputScaled = np.multiply(inputDF.values, np.ones(inputDF.shape) * scaling_array.T)
        np.nan_to_num(inputScaled)
        (_, componentsMatrix, _) = self.getPCAComponents(inputScaled)

        if len(componentsMatrix) < numberOfColumnsThreshold:
            complementaryColumns = np.zeros((numberOfColumnsThreshold - len(componentsMatrix), len(componentsMatrix[0])))
            componentsMatrix = np.vstack((componentsMatrix, complementaryColumns))

        return (scaling_array, np.mean(inputScaled, axis=0), componentsMatrix[ : numberOfColumnsThreshold, :])
    
    def __computeImputationValues(self, dataFrame, nullValues, calculateImputationValues):
        outputFrame = dataFrame
        if len(self.columns) <= 0:
            return dataFrame
        imputedColumns = list(self.columns.keys())
        
        dret = self.defaultValuesForNumerical(imputedColumns)
        for column in self.columns:
            if column not in dataFrame:
                continue
            if column in nullValues and nullValues[column] == 0:
                continue
            if column in dret['noImputation']:
                print 'Not imputed column:' + column
                continue
            if column in dret['zeroValues']:
                self.imputationValues[column] = 0
                outputFrame[column] = outputFrame[column].fillna(0)
                print 'Zero imputed column:' + column
                continue
            if column in dret['max']:
                if column in self.columnsMax:
                    self.imputationValues[column] = self.columnsMax[column]
                    outputFrame[column] = outputFrame[column].fillna(self.columnsMax[column])
                else:
                    self.imputationValues[column] = 0
                    outputFrame[column] = outputFrame[column].fillna(0)
                print 'Max imputed column:' + column
                continue
            self.imputationValues[column] = 0
            outputFrame[column] = outputFrame[column].fillna(0)
            print 'Zero imputed column (rarely):' + column
        return outputFrame
    
    def defaultValuesForNumerical(self, hd):  # includes float,integer,double
        ProductStrings0 = ['RevenueMomentum3', 'Revenue', 'Units', 'RevenueRollingSum6', 'Span']
        # SPAN SHOULD BE TREATED AS A CATEGORICAL VALUE, use median or use smallest non-zerovalue

        # zeroMapx: list of all product derived variables in playmaker whose default value should be zero
        # noImputation: this is a list of all fields for which imputation makes no sense
        # spanValues: this is a list of all product values with span
        zeroMapx = [x for x in hd if any([y.lower() in x.lower()  and 'product_' in x.lower() for y in ProductStrings0])]
        noImputation = [x for x in hd if x.lower() in ['analyticpurchasestate_id', 'leaccount_id', 'period_id', 'train', 'target', '__revenue', 'offset']]
        spanValues = [x for x in hd if 'product_' in x.lower() and '_span' in x.lower()]
        # if len(spanValues) != 0:  print "ERROR: SPAN IS CATEGORICAL"
        # hdx:  all variables not in previous 3, expect to be data cloud
        hdx = [x for x in hd if not any([x in y for y in [noImputation, zeroMapx, spanValues]])]
        remainder = [x for x in hd if x not in zeroMapx + noImputation + spanValues + hdx]
        dret = {}
        dret['max'] = [x for x in hdx if 'rank' in x.lower() and 'change' not in x.lower()]
        dret['zeroValues'] = zeroMapx + [x for x in hdx if x not in dret['max']]
        dret['noImputation'] = noImputation
        return dret
        
    def __computeImputationValues2(self, dataFrame, nullValues, calculateImputationValues):
        outputFrame = dataFrame
        if len(self.columns) > 0:
            expectedLabelValue = 0.0
            if self.targetColumn in outputFrame and calculateImputationValues == True:
                expectedLabelValue = self.__getExpectedLabelValue(outputFrame, self.targetColumn) 
                 
            for column, value in self.columns.iteritems():
                if column in outputFrame:
                    try:
                        if nullValues[column] > 0 and calculateImputationValues == True:
                            imputationBins = self.__createBins(outputFrame[column], outputFrame[self.targetColumn])
                            imputationValue = self.__matchValue(expectedLabelValue, imputationBins)
                            self.imputationValues[column] = imputationValue
         
                        outputFrame[column] = outputFrame[column].fillna(self.imputationValues[column])
                    except KeyError:
                        self.imputationValues[column] = value
                        outputFrame[column] = outputFrame[column].fillna(self.imputationValues[column])
        return outputFrame
        
    def __getExpectedLabelValue(self, dataFrame, targetColumn):
        zeroLabels = (dataFrame[targetColumn] == 0).sum()
        oneLabels = (dataFrame[targetColumn] == 1).sum()
        expectedLabelValue = float(oneLabels) / (oneLabels + zeroLabels)
        return expectedLabelValue
        
    def getTransformedMatrix(self, scalingArray, meanColumn, componentMatrix, originalMatrix):
        transformedMatrix = np.ndarray(shape=(0, 0)) 
          
        if (len(originalMatrix) > 0):
            scaledMatrix = np.multiply(originalMatrix, np.ones(originalMatrix.shape) * scalingArray.T)
            centeredMatrix = scaledMatrix - np.ones(originalMatrix.shape) * meanColumn.T
            transformedMatrix = np.dot(centeredMatrix, componentMatrix.T)
        return transformedMatrix
        
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
            logger.warn("Warning: Number of records and number of labels are different.")
     
        pairs = []
        numberOfPoints = min(len(x), len(y))
        for i in range(numberOfPoints):
            if pd.isnull(x[i]) == False:
                pairs.append([x[i], y[i]])
             
        if numBins > len(pairs) - 1 and len(pairs) != 0:
            numBins = len(pairs) - 1
     
        indBins = self.__createIndexSequence(len(pairs), numBins)
        def mvPair(k):
            return self.__meanValuePair([pairs[i] for i in range(indBins[k], indBins[k + 1])])
        return pd.Series([mvPair(b) for b in range(len(indBins) - 1)])
     
    def __matchValue(self, yvalue, binPairs):
        def absFn(x): return(x if x > 0 else (-x))
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
        ind = [i for i, x in enumerate(matches) if x == min(matches)][0]
        valuePair = binPairs[ind]
             
        splitValue = binPairs[1][0] - binPairs[0][0]
        adjValue = 0
        if valuePair == binPairs[0]:
            adjValue = -2.0 * splitValue
        if valuePair == binPairs[numBins - 1]:
            adjValue = 2.0 * splitValue
        return valuePair[0] + adjValue
  
class RevenueColumnTransformStep(PipelineStep):
    columns = OrderedDict()

    def __init__(self, enumMappings):
        self.columns = enumMappings
         
    def transform(self, dataFrame, configMetadata, test):
        if len(self.columns) == 0:
            return dataFrame
        for column in self.columns:
                if column not in dataFrame:
                    continue
                if re.match("Product_.*_Revenue$|Product_.*_RevenueRollingSum6$|Product_.*_Units$", column):
                    self.logRevenueColumnWithBooleanPositiveSimple(dataFrame, column)
                if re.match("Product_.*_RevenueMomentum3$", column):
                    self.logRevenueColumnWithBooleanNegativeSimple(dataFrame, column)
                    
        return dataFrame
    
    def logRevenueColumnWithBooleanPositiveSimple(self, dataFrame, column):
        dataFrame[column] = dataFrame[column].apply(lambda x : np.NaN if x <= 0 or x == None else x)
        dataFrame[column] = dataFrame[column].apply(lambda x : 0 if np.isnan(x) else x)
        dataFrame[column] = dataFrame[column].apply(lambda x :  x)
        dataFrame[column] = dataFrame[column].apply(lambda x : x if x != 0 else np.NaN)
    
    def logRevenueColumnWithBooleanNegativeSimple(self, dataFrame, column):
        dataFrame[column] = dataFrame[column].apply(lambda x : np.NaN if x == 0 or x == None else x)
        dataFrame[column] = dataFrame[column].apply(lambda x : 0 if np.isnan(x) else x)
        dataFrame[column] = dataFrame[column].apply(lambda x : x)
        dataFrame[column] = dataFrame[column].apply(lambda x : x if x != 0 else np.NaN)
    

class EVModelStep(PipelineStep):
    model = None
    modelInputColumns = []
    scoreColumnName = None
    revenueColumnName_ = None
    
    def __init__(self, model=None, modelInputColumns=None, revenueColumnName=None, scoreColumnName="Score"):
        self.model = model
        self.modelInputColumns = modelInputColumns
        self.scoreColumnName = scoreColumnName
        self.revenueColumnName_ = revenueColumnName
        self.setModelStep(True)
           
    def clone(self, model, modelInputColumns, revenueColumnName, scoreColumnName="Score"):
        return EVModelStep(model, modelInputColumns, revenueColumnName, scoreColumnName)
       
    def transform(self, dataFrame, configMetadata, test):
        outputFrame = pd.DataFrame(columns=["Score", "PredictedRevenue"])
        outputFrame["Score"] = dataFrame[self.scoreColumnName]
        if (self.revenueColumnName_ == None):
            outputFrame["PredictedRevenue"] = 0
            return outputFrame
          
        revenueColumn = self.model.predict_regression(dataFrame[self.modelInputColumns])
        if revenueColumn != None:
            outputFrame["PredictedRevenue"] = revenueColumn
            outputFrame["PredictedRevenue"] = outputFrame["PredictedRevenue"].apply(lambda x : x)
        else:
            outputFrame["PredictedRevenue"] = 0
        return outputFrame

