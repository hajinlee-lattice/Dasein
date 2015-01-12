from collections import defaultdict
import logging

import encoder
import numpy as np
import pandas as pd
from pipelinefwk import PipelineStep


logging.basicConfig(level = logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='pipeline')

def dictfreq(doc):
    freq = defaultdict(int)
    freq[doc] += 1
    return freq

class EnumeratedColumnTransformStep(PipelineStep):
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

class ColumnReductionStep(PipelineStep):
    outputColumns_ = []
    postScoreStep_ = False
    
    def __init__(self, outputColumns=[]):
        self.outputColumns_ = outputColumns
    
    def isPostScoreStep(self):
        return self.postScoreStep_
    
    def setPostScoreStep(self, postScoreStep):
        self.postScoreStep_ = postScoreStep
        
    def transform(self, dataFrame):
        outputFrame = dataFrame[self.outputColumns_]
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
        
class BucketingStep(PipelineStep):
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
            
class EVModelStep(PipelineStep):
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

