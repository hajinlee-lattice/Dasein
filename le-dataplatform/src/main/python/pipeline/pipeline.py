import logging

import columntransform
import encoder
from pipelinefwk import ModelStep
from pipelinefwk import Pipeline

logger = logging.getLogger(name='pipeline')
   
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
                value["hashValue"] = encoder.encode(value["hashValue"])
   
def setupPipeline(pipelineDriver, metadata, stringColumns, targetColumn):
    (categoricalColumns, continuousColumns) = getDecoratedColumns(metadata)
    # stringColumns refer to the columns that are categorical from the physical schema
    # categoricalColumns refer to the columns that are categorical from the metadata
    # We need to transform the physical strings into numbers
    columnsToTransform = set(stringColumns - set(categoricalColumns.keys()))

    pipelineFilePaths = [pipelineDriver]
    colTransform = columntransform.ColumnTransform(pathToPipelineFiles=pipelineFilePaths)
    steps = colTransform.buildPipelineFromFile(stringColumns=stringColumns, \
                                               categoricalColumns=categoricalColumns, \
                                               continuousColumns=continuousColumns, \
                                               targetColumn=targetColumn, \
                                               columnsToTransform=columnsToTransform)

    pipeline = Pipeline(steps)

    scoringSteps = steps + [ModelStep()]
    scoringPipeline = Pipeline(scoringSteps)

    return pipeline, scoringPipeline