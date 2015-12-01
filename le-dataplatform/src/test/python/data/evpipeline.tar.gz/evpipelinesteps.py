import encoder
import numpy as np
import pandas as pd
import random as rd
import statsmodels.tsa.ar_model as am
 
from pipelinefwk import PipelineStep
from pipelinefwk import get_logger
from sklearn.decomposition import PCA
 
logger = get_logger("evpipeline")
  
def get_predicted_revenue(row, colList):
    y = row[colList].as_matrix()
    y = y[y > 0]
    rev = 0
      
    try:
        mlag = len(y) / 2
        if mlag > 2:
            model = am.AR(y)
            results = model.fit(maxlag=mlag, ic='aic', trend='c', method='cmle')
            rev = results.predict().item(-1)
            if rev < 0.01:
                rev = 0
        elif len(y) == 0:
            rev = 0
        else:
            rev = np.mean(y)
    except:
        logger.info("No convergence, so using average")
        rev = np.mean(y)
        return rev
    return rev
  
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
                    logger.warn("Column %s cannot be transformed since it is not in the data frame." % column)
        return dataFrame
      
class ImputationStep(PipelineStep):
    enumMappings_ = dict()
    imputationValues = {}
    targetColumn = ""    
    scaling_array = []
    nullColmean = []
    component_matrix = []

    def __init__(self, enumMappings, imputationValues, scalingArray, meanColumn, componentMatrix, targetCol):
        self.enumMappings_ = enumMappings
        self.imputationValues = imputationValues
        self.targetColumn = targetCol
        self.scalingArray = scalingArray
        self.meanColumn = meanColumn
        self.componentMatrix = componentMatrix
 
    def transform(self, dataFrame):
        calculateImputationValues = True
        if len(self.imputationValues) != 0:
            calculateImputationValues = False
 
        outputFrame, nullValues = self.generateTransformedBooleanColumns(dataFrame, calculateImputationValues)
        outputFrame = self.imputeValues(outputFrame, nullValues, calculateImputationValues)
        return outputFrame
    
    def generateTransformedBooleanColumns(self, dataFrame, calculateImputationValues):
        outputFrame = dataFrame
        nullCols, nullValues = self.generateIsNullColumns(outputFrame)
 
        if calculateImputationValues:
            (self.scalingArray, self.meanColumn, self.componentMatrix) = self.nullValuePCA(nullCols, outputFrame[self.targetColumn])
             
        nullCol_train_tranformed = self.getTranformedMatrix(self.scalingArray, self.meanColumn, self.componentMatrix, nullCols)
        nullCol_train_tranformed = pd.DataFrame(nullCol_train_tranformed)
        outputFrame = pd.concat([outputFrame, nullCol_train_tranformed], axis = 1)
 
        del nullCols
        return outputFrame, nullValues
    
    def generateIsNullColumns(self, dataFrame):
        nullValues = {}
        outputFrame = dataFrame
        nullColsFrame = pd.DataFrame()
         
        if len(self.enumMappings_) > 0:
            for column, value in self.enumMappings_.iteritems():
                if column in outputFrame:
                    isNullColumn, nullCount = self.getIsNullColumn(outputFrame[column])
                    nullValues[column] = nullCount
                    nullColsFrame[column + "_isNull"] = pd.Series(isNullColumn)
        return nullColsFrame, nullValues
             
    def getIsNullColumn(self, dataColumn):
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
        explainedVarianceRatio =  pca.explained_variance_ratio_
        componentsMatrix = pca.components_
        X_transformed = pca.transform(X)
         
        return (explainedVarianceRatio, componentsMatrix, X_transformed)  
    
    def nullValuePCA(self, inputDF, eventCol, thresholdVariance=0.98, numberOfColumnsThreshold=5):
        scaling_array = self.getScalingForPCA(inputDF, eventCol)
        inputScaled = np.multiply(inputDF.values, np.ones(inputDF.shape) * scaling_array.T)        
        (explainedVarianceRatio, componentsMatrix, inputTransformed) = self.getPCAComponents(inputScaled)
        indexOfMaxVariance = self.getindexofMaxVariance(explainedVarianceRatio, thresholdVariance)
        numberOfColumns = min(indexOfMaxVariance, numberOfColumnsThreshold)
         
        return (scaling_array, np.mean(inputScaled, axis = 0), componentsMatrix[ : numberOfColumns, :])

    def imputeValues(self, dataFrame, nullValues, calculateImputationValues):
        outputFrame = dataFrame
        if len(self.enumMappings_) > 0:
            if self.targetColumn in outputFrame and calculateImputationValues == True:
                expectedLabelValue = self.getExpectedLabelValue(outputFrame, self.targetColumn) 
             
            for column, value in self.enumMappings_.iteritems():
                if column in outputFrame:
                    if nullValues[column] > 0 and calculateImputationValues == True:
                        imputationBins = self.createBins(outputFrame[column], outputFrame[self.targetColumn])
                        imputationValue = self.matchValue(expectedLabelValue, imputationBins)
                        self.imputationValues[column] = imputationValue
 
                    try:
                        outputFrame[column] = outputFrame[column].fillna(self.imputationValues[column])
                    except KeyError:
                        self.imputationValues[column] = value
                        outputFrame[column] = outputFrame[column].fillna(self.imputationValues[column])
        return outputFrame
       
    def getExpectedLabelValue(self, dataFrame, targetColumn):
        zeroLabels = (dataFrame[targetColumn] == 0).sum()
        oneLabels = (dataFrame[targetColumn] == 1).sum()
        expectedLabelValue = float(oneLabels) / (oneLabels + zeroLabels)
        return expectedLabelValue
    
    def getTranformedMatrix(self, scalingArray, meanColumn, componentMatrix, originalMatrix):
        scaledMatrix = np.multiply(originalMatrix, np.ones(originalMatrix.shape) * scalingArray.T)
        centeredMatrix = scaledMatrix - np.ones(originalMatrix.shape) * meanColumn.T
        transformedMatrix = np.dot(centeredMatrix, componentMatrix.T)
        return transformedMatrix
    
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
 
    def createBins(self, x, y, numBins=20):
        if len(x) != len(y):
             print "Warning: Number of records and number of labels are different."
 
        pairs = []
        numberOfPoints = min(len(x), len(y))
        for i in range(numberOfPoints):
            if pd.isnull(x[i]) == False:
                pairs.append([x[i], y[i]])
         
        if numBins > len(pairs) -1 and len(pairs) != 0:
            numBins = len(pairs)-1
 
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
 
class EVModelStep(PipelineStep):
      
    def __init__(self, props):
        PipelineStep.__init__(self, props)
        self.setPostScoreStep(True)
          
    def getRequiredProperties(self):
        return ["provenanceProperties"]
          
    def transform(self, dataFrame):
        provenanceProps = self.props_["provenanceProperties"]
        colList = provenanceProps["EVModelColumns"].split(",")
        colList = [x for x in colList if x in dataFrame]
          
        outputFrame = pd.DataFrame(columns=["Score", "ExpectedRevenue"])
        outputFrame["Score"] = dataFrame["Score"]
        outputFrame["ExpectedRevenue"] = dataFrame.apply(lambda row: get_predicted_revenue(row, colList), axis=1)
        return outputFrame