from collections import OrderedDict
import logging

import columntransform
import encoder
from pipelinefwk import ModelStep
from pipelinefwk import Pipeline
from pipelinesteps import ColumnTypeConversionStep
from pipelinesteps import EnumeratedColumnTransformStep
from pipelinesteps import ImputationStep


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
   
def setupPipeline(metadata, stringColumns, targetColumn):
    (categoricalColumns, continuousColumns) = getDecoratedColumns(metadata)
    # stringColumns refer to the columns that are categorical from the physical schema
    # categoricalColumns refer to the columns that are categorical from the metadata
    # We need to transform the physical strings into numbers
    columnsToTransform = set(stringColumns - set(categoricalColumns.keys()))

    steps = None
    stepsFromJSONFile = None
    stepsFromPythonFile = None

    try:
        stepsFromPythonFile = [EnumeratedColumnTransformStep(categoricalColumns), ColumnTypeConversionStep(columnsToTransform), ImputationStep(OrderedDict(continuousColumns), {}, targetColumn)]
        steps = stepsFromPythonFile
    except Exception as e:
        stepsFromPythonFile = None
        logger.error("Couldn't load Pipeline from Python file: " + e)

    try:
        pipelineFilePaths = ["./lepipeline.tar.gz/pipeline.json"]
        colTransform = columntransform.ColumnTransform(pathToPipelineFiles=pipelineFilePaths)
        stepsFromJSONFile = colTransform.buildPipelineFromFile(stringColumns = stringColumns, \
                                                               categoricalColumns=categoricalColumns, \
                                                               continuousColumns=continuousColumns, \
                                                               targetColumn=targetColumn, \
                                                               columnsToTransform=columnsToTransform)

    except Exception as e:
        stepsFromJSONFile = None
        logger.exception("Couldn't load Pipeline from JSON file. Exception: ")

    if stepsFromJSONFile:
        steps = stepsFromJSONFile
    else:
        steps = stepsFromPythonFile

    pipeline = Pipeline(steps)

    scoringSteps = steps + [ModelStep()]
    scoringPipeline = Pipeline(scoringSteps)

    return pipeline, scoringPipeline