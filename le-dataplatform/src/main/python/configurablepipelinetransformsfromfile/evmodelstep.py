import math
import pandas as pd

from pipelinefwk import PipelineStep
from pipelinefwk import get_logger
  
logger = get_logger("evpipeline")

class EVModelStep(PipelineStep):
    model_ = None
    modelInputColumns_ = []
    scoreColumnName_ = None
    revenueColumnName_ = None
    def __init__(self, model=None, modelInputColumns=None, revenueColumnName=None, scoreColumnName="Score"):
        self.model_ = model
        self.modelInputColumns_ = modelInputColumns
        self.scoreColumnName_ = scoreColumnName
        self.revenueColumnName_ = revenueColumnName
        self.setModelStep(True)
           
    def clone(self, model, modelInputColumns, revenueColumnName, scoreColumnName="Score"):
        return EVModelStep(model, modelInputColumns, revenueColumnName, scoreColumnName)
       
    def transform(self, dataFrame):
          
        outputFrame = pd.DataFrame(columns=["Score", "PredictedRevenue"])
        outputFrame["Score"] = dataFrame[self.scoreColumnName_]
        if (self.revenueColumnName_ == None):
            outputFrame["PredictedRevenue"] = 0
            return outputFrame
          
        revenueColumn = self.model_.predict_regression(dataFrame[self.modelInputColumns_])  
        if (revenueColumn != None):      
            outputFrame["PredictedRevenue"] = revenueColumn
            outputFrame["PredictedRevenue"] = outputFrame["PredictedRevenue"].apply(lambda x : math.exp(x) - 1.0)
        else:
            outputFrame["PredictedRevenue"] = 0
        return outputFrame