import math
import numpy as np
import re

from pipelinefwk import PipelineStep
from pipelinefwk import get_logger

from collections import OrderedDict

logger = get_logger("evpipeline")

class RevenueColumnTransformStep(PipelineStep):
    columns = OrderedDict()

    def __init__(self, enumMappings):
         self.columns = enumMappings
         
    def transform(self, dataFrame):
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
        dataFrame[column] = dataFrame[column].apply(lambda x :  math.log(1.0 + x))
        dataFrame[column] = dataFrame[column].apply(lambda x : x if x != 0 else np.NaN)
    
    def logRevenueColumnWithBooleanNegativeSimple(self, dataFrame, column):
        dataFrame[column] = dataFrame[column].apply(lambda x : np.NaN if x == 0 or x == None else x)
        dataFrame[column] = dataFrame[column].apply(lambda x : 0 if np.isnan(x) else x)
        dataFrame[column] = dataFrame[column].apply(lambda x : np.log(1.0 + x) if x >= 0 else -math.log(1.0 - x))
        dataFrame[column] = dataFrame[column].apply(lambda x : x if x != 0 else np.NaN)
    
    def logRevenueColumnWithBooleanPositive(self, dataFrame, column):
        dataFrame[column] = dataFrame[column].apply(lambda x : np.NaN if x <= 0 or x == None else x)
        dataFrame['Trx_Boolean_Positive_' + column] = dataFrame[column].apply(lambda x : 1 if np.isnan(x) else 0)
        dataFrame[column] = dataFrame[column].apply(lambda x : 0 if np.isnan(x) else x)
        dataFrame[column] = dataFrame[column].apply(lambda x :  math.log(1.0 + x))
    
    def logRevenueColumnWithBooleanNegative(self, dataFrame, column):
        dataFrame[column] = dataFrame[column].apply(lambda x : np.NaN if x == 0 or x == None else x)
        dataFrame['Trx_Boolean_Negative_' + column] = dataFrame[column].apply(lambda x : 1 if np.isnan(x) else 0)
        dataFrame[column] = dataFrame[column].apply(lambda x : 0 if np.isnan(x) else x)
        dataFrame[column] = dataFrame[column].apply(lambda x : np.log(1.0 + x) if x >= 0 else -math.log(1.0 - x))