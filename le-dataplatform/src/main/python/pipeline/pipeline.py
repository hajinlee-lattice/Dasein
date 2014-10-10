import encoder
from pipelinesteps import EnumeratedColumnTransformStep
from pipelinesteps import ImputationStep

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

def getDecoratedColumns(metadata):
    stringColumns = dict()
    continuousColumns = dict()
    transform = encoder.HashEncoder()
        
    for key, value in metadata.iteritems():
        if value[0]["Dtype"] == "STR":
            stringColumns[key] = transform
        else:
            continuousColumns[key] = value[0]["median"]
        
    return (stringColumns, continuousColumns)

def encodeCategoricalColumnsForMetadata(metadata):
    for _, values in metadata.iteritems():
        for value in values:
            if value["Dtype"] == "STR" and value["hashValue"] is not None:
                value["hashValue"] = encoder.transform(value["hashValue"])

def setupPipeline(metadata):
    (stringColumns, continuousColumns) = getDecoratedColumns(metadata)
    steps = [EnumeratedColumnTransformStep(stringColumns), ImputationStep(continuousColumns)]
    pipeline = Pipeline(steps)
    return pipeline


