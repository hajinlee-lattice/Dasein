import math
import pandas as pd

from pipelinefwk import PipelineStep
from pipelinefwk import get_logger
  
logger = get_logger("evpipeline")

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
        if self.revenueColumnName_ == None:
            outputFrame["PredictedRevenue"] = 0
            return outputFrame
          
        revenueColumn = self.model.predict_regression(dataFrame[self.modelInputColumns])
        if revenueColumn != None:
            outputFrame["PredictedRevenue"] = revenueColumn
            outputFrame["PredictedRevenue"] = outputFrame["PredictedRevenue"].apply(lambda x : math.exp(x) - 1.0)
        else:
            outputFrame["PredictedRevenue"] = 0
        return outputFrame