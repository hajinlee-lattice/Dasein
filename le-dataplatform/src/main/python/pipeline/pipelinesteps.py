from collections import defaultdict
import logging

import encoder
import numpy as np
import pandas as pd


logging.basicConfig(level = logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='pipeline')

def dictfreq(doc):
    freq = defaultdict(int)
    freq[doc] += 1
    return freq

class EnumeratedColumnTransformStep:
    _enumMappings = {}
    def __init__(self, enumMappings):
        self._enumMappings = enumMappings
        
    def transform(self, dataFrame):
        outputFrame = dataFrame
        for column, encoder in self._enumMappings.iteritems():
            if hasattr(encoder, 'classes_'):
                classSet = set(encoder.classes_.flat)
                outputFrame[column] = outputFrame[column].map(lambda s: 'NULL' if s not in classSet else s)
                encoder.classes_ = np.append(encoder.classes_, 'NULL')
            
            if column in outputFrame:
                logger.info("Transforming column %s." % column)
                outputFrame[column] = encoder.transform(outputFrame[column])
     
        return outputFrame

class EnumeratedColumnTransformStep2:
    _enumMappings = {}
    def __init__(self, enumMappings):
        self._enumMappings = enumMappings
        
    def transform(self, dataFrame):
        outputFrame = dataFrame
        if self._enumMappings['method'] == 1:
            for column, encoder in self._enumMappings['list'].iteritems():
                outputFrame[column] = outputFrame[column].fillna('missing')
                df = pd.DataFrame(encoder.transform(dictfreq(d) for d in outputFrame[column]).todense(), columns=(column + '_' + p for p in encoder.feature_names_))
                outputFrame = outputFrame.join(df)
                outputFrame = outputFrame.drop(column, axis=1)
        elif self._enumMappings['method'] == 2:
            for column, encoder in self._enumMappings['list'].iteritems():
                outputFrame[column] = outputFrame[column].map(lambda s: 'NULL' if s not in encoder.classes_.tolist() else s)
                encoder.classes_ = np.append(encoder.classes_, 'NULL')
                outputFrame[column] = encoder.transform(outputFrame[column])
         
        return outputFrame
    
class ColumnReductionStep:
    outputColumns_ = []
    def __init__(self, outputColumns=[]):
        self.outputColumns_ = outputColumns
    
    def transform(self, dataFrame):
        outputFrame = dataFrame[self.outputColumns_]
        return outputFrame

class ColumnTypeConversionStep:
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
    
class ImputationStep:
    _enumMappings = dict()
    def __init__(self, enumMappings):
        self._enumMappings = enumMappings

    def transform(self, dataFrame):
        outputFrame = dataFrame
        if len(self._enumMappings) > 0:
            for column, value in self._enumMappings.iteritems():
                if column in outputFrame:
                    outputFrame[column] = outputFrame[column].fillna(value)
        return outputFrame
        
class BucketingStep:
    bucketRanges_ = []
    
    def __init__(self, scoreColumn, bucketColumn, bucketRanges, defaultBucket):
        self.bucketRanges_ = bucketRanges
        self.scoreColumn_ = scoreColumn
        self.bucketColumn_ = bucketColumn
        self.defaultBucket_ = defaultBucket
        
    def transform(self, dataFrame):
        outputFrame = dataFrame
        
        func = lambda x: self.applyBuckets(x)
        outputFrame[self.bucketColumn_] = outputFrame[self.scoreColumn_].apply(func)
        return outputFrame

    def applyBuckets(self, x):
        for bucketName, bucketRange in self.bucketRanges_:
            if bucketRange[0] <= x and bucketRange[1] > x:
                return bucketName
        return self.defaultBucket_
            
class EVModelStep:
    classificationModel_ = None
    regressionModel_ = None
    modelInputColumns_ = []
    outputColumns_ = []
    scoreColumnName_ = ''
    
    def __init__(self, classificationModel, regressionModel, modelInputColumns, outputColumns=[], scoreColumnName="Score"):
        self.classificationModel_ = classificationModel
        self.regressionModel_ = regressionModel
        self.modelInputColumns_ = modelInputColumns
        self.outputColumns_ = outputColumns
        self.scoreColumnName_ = scoreColumnName
        
    def transform(self, dataFrame):
        dataFrame = dataFrame.convert_objects()
        dataFrame.fillna(0, inplace=True)
        probabilityColumn = self.classificationModel_.predict_proba(dataFrame[self.modelInputColumns_])[:, 1]
        valueColumn = self.regressionModel_.predict(dataFrame[self.modelInputColumns_])
        outputFrame = pd.DataFrame(probabilityColumn, columns=["Probability"])
        outputFrame['Value'] = valueColumn
        outputFrame[self.scoreColumnName_] = outputFrame['Probability'] * outputFrame['Value']
        for columnName in self.outputColumns_:
            outputFrame[columnName] = pd.Series(dataFrame[columnName].values, index=outputFrame.index)
        return outputFrame

