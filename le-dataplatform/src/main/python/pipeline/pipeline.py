import logging

import columntransform
import encoder
from pipelinefwk import ModelStep
from pipelinefwk import Pipeline
from rulefwk import DataRulePipeline

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

def setupSteps(pipelineDriver, pipelineLib, metadata, stringColumns, targetColumn, pipelineProps=""):
    (categoricalColumns, continuousColumns) = getDecoratedColumns(metadata)
    # stringColumns refer to the columns that are categorical from the physical schema
    # categoricalColumns refer to the columns that are categorical from the metadata
    # We need to transform the physical strings into numbers
    columnsToTransform = set(stringColumns - set(categoricalColumns.keys()))

    colTransform = columntransform.ColumnTransform(pathToPipelineFiles=[pipelineDriver])
    (names, steps) = colTransform.buildPipelineFromFile(pipelinePath="./" + pipelineLib, \
                                               stringColumns=stringColumns, \
                                               categoricalColumns=categoricalColumns, \
                                               continuousColumns=continuousColumns, \
                                               targetColumn=targetColumn, \
                                               columnsToTransform=columnsToTransform, \
                                               profile=metadata)

    # If properties are empty, don't try and set values
    disabledSteps = []
    if pipelineProps:
        try:
            props = dict((u.split("=")[0], u.split("=")[1]) for u in pipelineProps.split(" "))
            stepMap = dict((names[i], steps[i]) for i in xrange(len(steps)))
            for prop, value in props.iteritems():
                try:
                    tokens = prop.split(".")
                    step = stepMap[tokens[0]]
                    if step is not None:
                        if tokens[1] == 'enabled' and value.lower() == 'false':
                            disabledSteps.append(step)
                            continue
                        currentValue = getattr(step, tokens[1])
                        setattr(step, tokens[1], (type(currentValue))(value))
                except Exception as propError:
                    logger.error(str(propError))
        except Exception as e:
            logger.error(str(e))

        for disabledStep in reversed(disabledSteps):
            logger.info('Remove disabled step ' + disabledStep.__class__.__name__)
            steps.remove(disabledStep)

    return steps

def setupPipeline(pipelineDriver, pipelineLib, metadata, stringColumns, targetColumn, pipelineProps=""):
    steps = setupSteps(pipelineDriver, pipelineLib, metadata, stringColumns, targetColumn, pipelineProps)

    pipeline = Pipeline(steps)

    scoringSteps = steps + [ModelStep()]
    scoringPipeline = Pipeline(scoringSteps)

    return pipeline, scoringPipeline

def setupRulePipeline(pipelineDriver, pipelineLib, metadata, stringColumns, targetColumn, pipelineProps=""):
    steps = setupSteps(pipelineDriver, pipelineLib, metadata, stringColumns, targetColumn, pipelineProps)

    pipeline = DataRulePipeline(steps)

    return pipeline