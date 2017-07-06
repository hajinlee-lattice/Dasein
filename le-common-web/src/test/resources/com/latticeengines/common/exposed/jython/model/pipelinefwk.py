import logging
import sys
 
import pandas as pd
 
def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

def create_column(name, dataType):
    return { "name": name, "type": dataType}
 
class Pipeline:
    pipelineSteps = []
    def __init__(self, pipelineSteps):
        self.pipelineSteps = pipelineSteps
     
    def getPipeline(self):
        return self.pipelineSteps
     
    def predict(self, dataFrame):
        transformed = dataFrame
        for step in self.pipelineSteps:
            transformed = step.transform(transformed)
        return transformed
    
class PipelineStep:
    modelStep_ = False
    props_ = {}
 
    def __init__(self, props):
        self.props_ = props
     
    def isModelStep(self):
        return self.modelStep_
     
    def setModelStep(self, modelStep):
        self.modelStep_ = modelStep
         
    def transform(self, dataFrame): pass
 
    def setProperty(self, propertyName, propertyValue):
        self.props_[propertyName] = propertyValue
         
    def getProperty(self, propertyName):
        return self.props_[propertyName]
    
    def getInputColumns(self):
        return []
    
    # Returns a list of a map to array of strings, where key is the output column name
    # and value is a list of column names as input
    def getOutputColumns(self):
        return []
    
    def getRTSMainModule(self):
        return None 

    def getRTSArtifacts(self):
        return []
    
class ModelStep(PipelineStep):
    model_ = None
    modelInputColumns_ = []
    outputColumns_ = []
    scoreColumnName_ = ''
     
    def getModel(self):
        return self.model_
     
    def getModelInputColumns(self):
        return self.modelInputColumns_
     
    def __init__(self, model = None, modelInputColumns = None, scoreColumnName="Score"):
        self.model_ = model
        self.modelInputColumns_ = modelInputColumns
        self.scoreColumnName_ = scoreColumnName
        self.setModelStep(True)
         
    def clone(self, model, modelInputColumns, revenueColumnName, scoreColumnName = "Score"):
        return ModelStep(model, modelInputColumns, scoreColumnName)
    
    def transform(self, dataFrame):
        dataFrame = dataFrame.convert_objects(convert_numeric=True)
        dataFrame.fillna(0, inplace=True)
             
        for columnName, columnData in dataFrame.iteritems():
            if columnData.dtype == object:
                dataFrame[columnName] = 0
 
        if "verbose" in self.model_.__dict__:
            self.model_.verbose = 0
        scoreColumn = self.model_.predict_proba(dataFrame[self.modelInputColumns_])
        # Check number of event classes 
        index = 1 if len(scoreColumn[0]) == 2 else 0
        scoreColumn = scoreColumn[:, index]
        outputFrame = pd.DataFrame(scoreColumn, columns=[self.scoreColumnName_])
         
        for columnName in self.modelInputColumns_:
            outputFrame[columnName] = pd.Series(dataFrame[columnName].values, index=outputFrame.index)
             
        return outputFrame