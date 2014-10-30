import pandas as pd

class Pipeline:
    pipelineSteps_ = []
    def __init__(self, pipelineSteps):
        self.pipelineSteps_ = pipelineSteps
    
    def getPipeline(self):
        return self.pipelineSteps_
    
    def predict(self, dataFrame):
        transformed = dataFrame

        for step in self.pipelineSteps_:
            transformed = step.transform(transformed)
            
        return transformed

class ModelStep:
    model_ = None
    modelInputColumns_ = []
    outputColumns_ = []
    scoreColumnName_ = ''
    
    def getModel(self):
        return self.model_
    
    def getModelInputColumns(self):
        return self.modelInputColumns_
    
    def __init__(self, model, modelInputColumns, outputColumns=[], scoreColumnName="Score"):
        self.model_ = model
        self.modelInputColumns_ = modelInputColumns
        self.outputColumns_ = outputColumns
        self.scoreColumnName_ = scoreColumnName
        
    def transform(self, dataFrame):
        dataFrame = dataFrame.convert_objects(convert_numeric=True)
        dataFrame.fillna(0, inplace=True)
            
        for columnName, columnData in dataFrame.iteritems():
            if columnData.dtype == object:
                dataFrame[columnName] = 0
        
        scoreColumn = self.model_.predict_proba(dataFrame[self.modelInputColumns_])
        # Check number of event classes 
        index = 1 if len(scoreColumn[0]) == 2 else 0
        scoreColumn = scoreColumn[:, index]
        outputFrame = pd.DataFrame(scoreColumn, columns=[self.scoreColumnName_])
        
        for columnName in self.outputColumns_:
            outputFrame[columnName] = pd.Series(dataFrame[columnName].values, index=outputFrame.index)
            
        return outputFrame

