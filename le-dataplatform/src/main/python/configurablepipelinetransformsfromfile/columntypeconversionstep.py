import numpy as np
import pandas as pd

from pipelinefwk import PipelineStep
from pipelinefwk import get_logger
from pipelinefwk import create_column

logger = get_logger("pipeline")

class ColumnTypeConversionStep(PipelineStep):
    columnsToConvert = []
        
    def __init__(self, columnsToConvert=[]):
        self.columnsToConvert = columnsToConvert
    
    def transform(self, dataFrame, configMetadata, test):
        if len(self.columnsToConvert) > 0:
            for column in self.columnsToConvert:
                if column in dataFrame and dataFrame[column].dtype == np.object_:
                    dataFrame[column] = pd.Series(pd.lib.maybe_convert_numeric(dataFrame[column].as_matrix(), set(), coerce_numeric=True))
                else:
                    logger.info("Column %s cannot be transformed since it is not in the data frame." % column)
        return dataFrame
    
    def getColumns(self):
        return self.columnsToConvert
    
    def getOutputColumns(self):
        return [(create_column(k, "FLOAT"), [k]) for k in self.columnsToConvert]
    
    def getRTSMainModule(self):
        return "make_float"