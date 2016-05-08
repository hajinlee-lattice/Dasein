from collections import OrderedDict
import logging

import encoder
from evpipelinesteps import ColumnTypeConversionStep
from evpipelinesteps import EVModelStep
from evpipelinesteps import EnumeratedColumnTransformStep
from evpipelinesteps import ImputationStep
from evpipelinesteps import RevenueColumnTransformStep
from pipelinefwk import ModelStep
from pipelinefwk import Pipeline


logger = logging.getLogger(name="evpipeline")

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
   
def setupPipeline(pipelineDriver, pipelineLib, metadata, stringColumns, targetColumn, pipelineProps=""):
    (categoricalColumns, continuousColumns) = getDecoratedColumns(metadata)
    # stringColumns refer to the columns that are categorical from the physical schema
    # categoricalColumns refer to the columns that are categorical from the metadata
    # We need to transform the physical strings into numbers
    columnsToTransform = set(stringColumns - set(categoricalColumns.keys()))

    steps = [EnumeratedColumnTransformStep(categoricalColumns), \
             ColumnTypeConversionStep(columnsToTransform), \
             RevenueColumnTransformStep(OrderedDict(continuousColumns)), \
             ImputationStep(OrderedDict(continuousColumns), {}, [], [], [], targetColumn)]
    pipeline = Pipeline(steps)
      
    scoringSteps = steps + [ModelStep(), EVModelStep()]
    scoringPipeline = Pipeline(scoringSteps)
      
    return pipeline, scoringPipeline