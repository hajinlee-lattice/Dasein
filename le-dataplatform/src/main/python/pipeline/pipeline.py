import encoder
from pipelinefwk import Pipeline
from pipelinesteps import ColumnTypeConversionStep
from pipelinesteps import EnumeratedColumnTransformStep
from pipelinesteps import ImputationStep

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

def setupPipeline(metadata, stringColumns):
    (categoricalColumns, continuousColumns) = getDecoratedColumns(metadata)
    stringColumns = list(set(categoricalColumns.keys()) - stringColumns)
    steps = [EnumeratedColumnTransformStep(categoricalColumns), ColumnTypeConversionStep(stringColumns), ImputationStep(continuousColumns)]
    pipeline = Pipeline(steps)
    return pipeline
