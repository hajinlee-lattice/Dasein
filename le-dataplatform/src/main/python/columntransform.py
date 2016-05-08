'''
Contains all column tranformation functions, whether to apply to categorical or numerical columns
The actual function to apply are taken from model.json file
'''

import os
import json
import logging

import imp
from collections import OrderedDict

logger = logging.getLogger(name='columntransform')

class ColumnTransform(object):

    pipelineFileAsJson = ""
    columnTransformKey = u"columnTransformFiles"

    uniqueColumnTransformKey = u"Name"
    mainMethodNameKey = u"MainMethodName"
    uniqueColumnTransformNameKey = u"UniqueColumnTransformName"
    columnTransformFilePathKey = u"ColumnTransformFilePath"
    loadedModuleKey = u"LoadedModule"
    mainClassNameKey = u"MainClassName"
    namedParameterListToInitKey = u"NamedParameterListToInit"
    loadedObjectKey = u"LoadedObject"
    resultOfCallingMainMethodNameKey = u"ResultOfCallingMainMethodName"
    sortingKey = u'KeyWhenSortingByAscending'

    def __init__(self, pathToPipelineFiles = None):
        if pathToPipelineFiles is not None:
            try:
                for pipelineFile in pathToPipelineFiles:
                    logger.info("Opening file %s." % pipelineFile)
                    if os.path.isfile(pipelineFile):
                        with open(pipelineFile) as pipelineFileText:
                            self.pipelineFileAsJson = json.load(pipelineFileText)
                            logger.info("Pipeline json contents:")
                            logger.info(self.pipelineFileAsJson)
                            break
                    else:
                        logger.info("%s is not a file." % pipelineFile)
            except Exception as e:
                logger.warn(str(e))
                logger.exception("Could not load pipeline from provided path:" + str(pathToPipelineFiles))
        else:
            logger.info("Configurable pipeline not loaded because empty path provided: %s" % pathToPipelineFiles)
            pass

    def getPipelineAsJson(self):
        return self.pipelineFileAsJson

    def setPipelineAsJson(self, pipelineFileAsJson):
        self.pipelineFileAsJson = pipelineFileAsJson

    def checkJsonForCorrectness(self, jsonToProcess):
        try:
            if self.columnTransformKey not in jsonToProcess.keys():
                logger.error("Missing Required Parent Key:" + self.columnTransformKey + " Exception:" + jsonToProcess)
                return False

            testPass = True
            missingRequiredKey = None

            for _, value in jsonToProcess[self.columnTransformKey].iteritems():
                if self.uniqueColumnTransformKey not in value:
                    testPass = False
                    missingRequiredKey = self.uniqueColumnTransformKey
                if self.uniqueColumnTransformNameKey not in value:
                    testPass = False
                    missingRequiredKey = self.uniqueColumnTransformNameKey
                if self.columnTransformFilePathKey not in value:
                    testPass = False
                    missingRequiredKey = self.columnTransformFilePathKey
                if self.mainClassNameKey not in value:
                    testPass = False
                    missingRequiredKey = self.mainClassNameKey
                if self.namedParameterListToInitKey not in value:
                    testPass = False
                    missingRequiredKey = self.namedParameterListToInitKey

                if testPass == False:
                    logger.error("Missing Required Key: " + missingRequiredKey + " JSONToProcess:" + jsonToProcess)
                    return False

                return True
        except Exception:
            logger.exception("Caught Exception while checking Configurable Pipeline JSON for correctness")
            return False

    def buildPipelineFromFile(self, pipelinePath="./lepipeline.tar.gz",
                                       stringColumns=None, 
                                       categoricalColumns=None, 
                                       continuousColumns=None, 
                                       targetColumn=None, 
                                       columnsToTransform=None,
                                       profile=None):
        # Return Array that holds functions loaded from file
        pipelineAsArrayOfTransformClasses = []
        pipelineName = []

        try:
            jsonToProcess = self.pipelineFileAsJson;
            if jsonToProcess is None:
                logger.info("JsonToProcess is Empty. No transforms could be created.")
                return pipelineAsArrayOfTransformClasses

            if self.checkJsonForCorrectness(jsonToProcess) == False:
                logger.info("Couldn't validate JSON file %s for configurable pipeline." % jsonToProcess)
                return pipelineAsArrayOfTransformClasses

            for _, value in sorted(jsonToProcess[self.columnTransformKey].iteritems(), key=lambda item: item[1][self.sortingKey]):
                uniqueColumnTransformName = value[self.uniqueColumnTransformKey]

                columnTransformObject = {}
                columnTransformObject[self.uniqueColumnTransformNameKey] = value[self.uniqueColumnTransformNameKey]

                sourceFilePathLowerCased = str(value[self.columnTransformFilePathKey]).lower()
                columnTransformObject[self.columnTransformFilePathKey] = sourceFilePathLowerCased

                logger.info("Configurable Pipeline Current Path: %s" % str(os.getcwd()))
                fileObject, pathname, description = imp.find_module(uniqueColumnTransformName, [pipelinePath])
                columnTransformObject[self.loadedModuleKey] = imp.load_module(uniqueColumnTransformName, fileObject, pathname, description)
                logger.info("Loaded %s for Configurable Pipeline " % uniqueColumnTransformName) 

                mainClassName = value[self.mainClassNameKey]
                namedParameterList = value[self.namedParameterListToInitKey]
                kwargs = self.buildKwArgs(namedParameterList = namedParameterList, \
                                          stringColumns = stringColumns, \
                                          categoricalColumns=categoricalColumns, \
                                          continuousColumns=continuousColumns, \
                                          targetColumn=targetColumn, \
                                          columnsToTransform=columnsToTransform,
                                          profile=profile)
                loadedObject = getattr(columnTransformObject[self.loadedModuleKey], mainClassName)(**kwargs)
                columnTransformObject[self.loadedObjectKey] = loadedObject

                loadedModule = getattr(loadedObject, "__init__")(**kwargs)
                columnTransformObject[self.resultOfCallingMainMethodNameKey] = loadedModule

                pipelineName.append(uniqueColumnTransformName)
                pipelineAsArrayOfTransformClasses.append(loadedObject)

            logger.info("Finished Loading Configurable Pipeline")

            return (pipelineName, pipelineAsArrayOfTransformClasses)
        except Exception as e:
            logger.exception("Caught Exception while building Configurable Pipeline %s" % str(e))
            return None

    def buildKwArgs(self, namedParameterList=None, 
                            stringColumns=None,
                            categoricalColumns=None,
                            continuousColumns=None,
                            targetColumn=None,
                            columnsToTransform=None,
                            profile=None):
        kwargs = {}
        try:
            value = None

            for namedArgument, namedArgumentDataTypeOrValue in namedParameterList.iteritems():
                t = type(namedArgumentDataTypeOrValue)
                if t is not str and t is not unicode:
                    value = namedArgumentDataTypeOrValue
                elif namedArgumentDataTypeOrValue.lower() == "orderedDictContinuousColumns".lower():
                    if continuousColumns is None:
                        value = None
                    else:
                        value = OrderedDict(continuousColumns)
                elif namedArgumentDataTypeOrValue.lower() == "emptyDictionary".lower():
                    value = {}
                elif namedArgumentDataTypeOrValue.lower() == "categoricalColumns".lower():
                    value = categoricalColumns
                elif namedArgumentDataTypeOrValue.lower() == "emptyList".lower():
                    value = []
                elif namedArgumentDataTypeOrValue.lower() == "targetColumn".lower():
                    value = targetColumn
                elif namedArgumentDataTypeOrValue.lower() == "numericalColumns".lower():
                    value = continuousColumns
                elif namedArgumentDataTypeOrValue.lower() == "dataprofile".lower():
                    value = profile
                elif namedArgumentDataTypeOrValue.lower() == "columnsToTransform".lower():
                    if categoricalColumns is not None and stringColumns is not None:
                        value = set(stringColumns - set(categoricalColumns.keys()))
                    else:
                        value = None
                else:
                    value = namedArgumentDataTypeOrValue

                if namedArgument not in kwargs:
                    kwargs[namedArgument] = value
                else:
                    logger.warning("Warning: NamedArgument %s has already been assigned to kwargs." % namedArgument)

            return kwargs
        except Exception:
            logger.exception("Caught Exception while building KWArgs for Configurable Pipeline")
            raise