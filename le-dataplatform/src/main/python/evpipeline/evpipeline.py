from collections import OrderedDict
import logging

import encoder
from evpipelinesteps import ColumnTypeConversionStep
from evpipelinesteps import EVModelStep
from evpipelinesteps import EnumeratedColumnTransformStep
from evpipelinesteps import ImputationStep
from evpipelinesteps import RevenueColumnTransformStep
from highnumberuniquevaluesremovalstep import HighNumberUniqueValuesRemovalStep
from pipelinefwk import ModelStep
from pipelinefwk import Pipeline


logger = logging.getLogger(name="evpipeline")

def getDecoratedColumns(profile):
    stringColumns = dict()
    continuousColumnsMedian = dict()
    continuousColumnsMax = dict()
    continuousColumnsMin = dict()
    transform = encoder.HashEncoder()

    for key, value in profile.iteritems():
        if value[0]["Dtype"] == "STR":
            stringColumns[key] = transform
        else:
            continuousColumnsMedian[key] = value[0]["median"]
            if "globalMaxV" in value[0]:
                continuousColumnsMax[key] = value[0]["globalMaxV"]
            if "globalMinV" in value[0]:
                continuousColumnsMin[key] = value[0]["globalMinV"]

    return (stringColumns, continuousColumnsMedian, continuousColumnsMax, continuousColumnsMin)

def encodeCategoricalColumnsForMetadata(profile):
    for _, values in profile.iteritems():
        for value in values:
            if value["Dtype"] == "STR" and value["hashValue"] is not None:
                value["hashValue"] = encoder.encode(value["hashValue"])

def setupPipeline(pipelineDriver, pipelineLib, profile, columnMetadata, stringColumns, targetColumn, params, pipelineProps=""):
    (categoricalColumns, continuousColumnsMedian, continuousColumnsMax, continuousColumnsMin) = getDecoratedColumns(profile)
    # stringColumns refer to the columns that are categorical from the physical schema
    # categoricalColumns refer to the columns that are categorical from the metadata
    # We need to transform the physical strings into numbers
    columnsToTransform = set(stringColumns - set(categoricalColumns.keys()))

    steps = [HighNumberUniqueValuesRemovalStep(200, categoricalColumns, params, profile), \
             EnumeratedColumnTransformStep(categoricalColumns), \
             ColumnTypeConversionStep(columnsToTransform), \
             RevenueColumnTransformStep(OrderedDict(continuousColumnsMedian)), \
             ImputationStep(OrderedDict(continuousColumnsMedian), {}, [], [], OrderedDict(continuousColumnsMax), [], targetColumn)]
    pipeline = Pipeline(steps)

    scoringSteps = [x for x in steps if x.includeInScoringPipeline()]

    scoringSteps.extend([ModelStep(), EVModelStep()])
    scoringPipeline = Pipeline(scoringSteps)

    return pipeline, scoringPipeline