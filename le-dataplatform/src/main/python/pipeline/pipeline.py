import json
import logging

import columntransform
import encoder
from pipelinefwk import ModelStep
from pipelinefwk import Pipeline
from rulefwk import DataRulePipeline


logger = logging.getLogger(name='pipeline')

def getDecoratedColumns(profile):
    stringColumns = dict()
    continuousColumns = dict()
    transform = encoder.HashEncoder()

    for key, value in profile.iteritems():
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

def getObjectFromJSON(objectType, jsonString):
    try:
        objectFromJson = json.loads(jsonString)
        if type(objectFromJson) is dict and objectType is dict:
            return objectFromJson
        elif type(objectFromJson) is dict and objectType is list:
            return objectFromJson.keys()
        else:
            return objectFromJson
    except Exception as e:
        logger.error("Error converting JSON string. JsonString: {0}, objectType: {1}, Stacktrace below".format(jsonString, objectType))
        logger.error(e)

def setupSteps(pipelineDriver, pipelineLib, profile, columnMetadata, stringColumns, targetColumn, params, pipelineProps=""):
    (categoricalColumns, continuousColumns) = getDecoratedColumns(profile)
    # stringColumns refer to the columns that are categorical from the physical schema
    # categoricalColumns refer to the columns that are categorical from the metadata
    # We need to transform the physical strings into numbers
    columnsToTransform = set(stringColumns - set(categoricalColumns.keys()))

    keys = params['schema']['keys']
    customerColumns = set()
    if columnMetadata is not None:
        for column in columnMetadata:
            if column['Tags'] is None or column['ColumnName'] is None:
                continue
            if column['ColumnName'] in keys or column['ColumnName'] == targetColumn:
                continue
            if column['Tags'][-1] in ['Internal']:
                customerColumns.add(column['ColumnName'])

    allColumns = categoricalColumns.copy()
    allColumns.update(continuousColumns)

    colTransform = columntransform.ColumnTransform(pathToPipelineFiles=[pipelineDriver])
    (names, steps, defaultDisabledSteps) = colTransform.buildPipelineFromFile(pipelinePath="./" + pipelineLib, \
                                               stringColumns=stringColumns, \
                                               categoricalColumns=categoricalColumns, \
                                               continuousColumns=continuousColumns,
                                               customerColumns=customerColumns, \
                                               targetColumn=targetColumn, \
                                               columnsToTransform=columnsToTransform, \
                                               profile=profile, \
                                               allColumns=allColumns, \
                                               params=params)

    # If properties are empty, don't try and set values
    disabledSteps = []
    enabledSteps = []
    if pipelineProps:
        try:
            props = dict((u.split("=")[0], u.split("=")[1]) for u in pipelineProps.split(" "))
            stepMap = dict((names[i], steps[i]) for i in xrange(len(steps)))
            for prop, value in props.iteritems():
                try:
                    tokens = prop.split(".")
                    if tokens[0] in stepMap:
                        step = stepMap[tokens[0]]
                        if step is not None:
                            if tokens[1] == 'enabled' and value.lower() == 'false':
                                disabledSteps.append(step)
                                continue
                            if tokens[1] == 'enabled' and value.lower() == 'true':
                                enabledSteps.append(step)
                                continue
                            currentValue = getattr(step, tokens[1])
                            if type(currentValue) not in [type(list()), type(dict())]:
                                setattr(step, tokens[1], (type(currentValue))(value))
                            else:
                                try:
                                    objectFromJson = getObjectFromJSON(type(currentValue), value)
                                    setattr(step, tokens[1], objectFromJson)
                                except Exception as e:
                                    logger.error("Couldn't set object property correctly. Stack trace below")
                                    logger.error(e)
                except Exception as propError:
                    logger.error(str(propError))
        except Exception as e:
            logger.error(str(e))

    for disabledStep in reversed(disabledSteps):
        logger.info('Remove disabled step ' + disabledStep.__class__.__name__)
        steps.remove(disabledStep)

    for disabledStep in reversed(defaultDisabledSteps):
        if disabledStep in steps and disabledStep not in enabledSteps:
            logger.info('Remove disabled step ' + disabledStep.__class__.__name__)
            steps.remove(disabledStep)

    return steps

def setupPipeline(pipelineDriver, pipelineLib, profile, columnMetadata, stringColumns, targetColumn, params, pipelineProps=""):
    steps = setupSteps(pipelineDriver, pipelineLib, profile, columnMetadata, stringColumns, targetColumn, params, pipelineProps)

    pipeline = Pipeline(steps)

    scoringSteps = [x for x in steps if x.includeInScoringPipeline()]
    scoringSteps.append(ModelStep())
    scoringPipeline = Pipeline(scoringSteps)

    return pipeline, scoringPipeline

def setupRulePipeline(pipelineDriver, pipelineLib, profile, columnMetadata, stringColumns, targetColumn, params, pipelineProps=""):
    steps = setupSteps(pipelineDriver, pipelineLib, profile, columnMetadata, stringColumns, targetColumn, params, pipelineProps)

    pipeline = DataRulePipeline(steps)

    return pipeline
