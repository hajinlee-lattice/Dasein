import encoder
from evpipelinesteps import ColumnTypeConversionStep
from evpipelinesteps import EVModelStep
from evpipelinesteps import EnumeratedColumnTransformStep
from evpipelinesteps import ImputationStep
from pipelinefwk import Pipeline


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

def setupPipeline(metadata, stringColumns, targetColumn):
    (categoricalColumns, continuousColumns) = getDecoratedColumns(metadata)
    # stringColumns refer to the columns that are categorical from the physical schema
    # categoricalColumns refer to the columns that are categorical from the metadata
    # We need to transform the physical strings into numbers
    columnsToTransform = set(stringColumns - set(categoricalColumns.keys()))
    evModelStep = EVModelStep({})
    steps = [EnumeratedColumnTransformStep(categoricalColumns), ColumnTypeConversionStep(columnsToTransform), ImputationStep(continuousColumns, targetColumn), evModelStep]
    pipeline = Pipeline(steps)
    return pipeline
