import encoder
import numpy as np
from pipelinefwk import PipelineStep
from pipelinefwk import get_logger
from pipelinefwk import create_column

logger = get_logger("pipeline")

class EnumeratedColumnTransformStep(PipelineStep):
    columns = {}
    columnList = []
    
    def __init__(self, columns):
        self.columns = columns
        if columns:
            self.columnList = columns.keys()
        else:
            self.columnList = []

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
    
    def getColumns(self):
        return self.columnList
    
    def getOutputColumns(self):
        return [(create_column(k, "LONG"), [k]) for k in self.columnList]

    def getRTSMainModule(self):
        return "encoder"