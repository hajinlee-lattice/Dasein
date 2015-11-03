from collections import defaultdict

import encoder
import numpy as np
import pandas as pd
import random as rd
from pipelinefwk import PipelineStep
from pipelinefwk import get_logger

logger = get_logger("pipeline")

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
    targetCol = '' 
    
    def __init__(self, enumMappings, targetCol):
        self._enumMappings = enumMappings
        self.targetColumn = targetCol

    def transform(self, dataFrame):
        outputFrame = dataFrame
        if len(self._enumMappings) > 0:
            for column, value in self._enumMappings.iteritems():
                if column in outputFrame:
                    outputFrame[column] = outputFrame[column].fillna(value)
        return outputFrame