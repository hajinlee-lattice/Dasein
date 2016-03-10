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
                    if os.path.isfile(pipelineFile):
                        with open(pipelineFile) as pipelineFileText:
                            self.pipelineFileAsJson = json.load(pipelineFileText)
                            break
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
                if self.mainMethodNameKey not in value:
                    testPass = False
                    missingRequiredKey = self.mainMethodNameKey
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

    def buildPipelineFromFile(self, stringColumns = None, categoricalColumns=None, continuousColumns=None, targetColumn=None, columnsToTransform=None):
        # Return Array that holds functions loaded from file
        pipelineAsArrayOfTransformClasses = []

        try:
            jsonToProcess = self.pipelineFileAsJson;
            if jsonToProcess is None:
                logger.info("JsonToProcess is Empty. No transforms could be created")
                return pipelineAsArrayOfTransformClasses

            if self.checkJsonForCorrectness(jsonToProcess) == False:
                logger.info("Couldn't validate JSON that created configurable pipeline")
                return pipelineAsArrayOfTransformClasses

            for _, value in sorted(jsonToProcess[self.columnTransformKey].iteritems(), key=lambda item: item[1][self.sortingKey]):
                uniqueColumnTransformName = value[self.uniqueColumnTransformKey]

                columnTransformObject = {}
                columnTransformObject[self.mainMethodNameKey] = value[self.mainMethodNameKey]
                columnTransformObject[self.uniqueColumnTransformNameKey] = value[self.uniqueColumnTransformNameKey]

                sourceFilePathLowerCased = str(value[self.columnTransformFilePathKey]).lower()
                columnTransformObject[self.columnTransformFilePathKey] = sourceFilePathLowerCased

                logger.info("Configurable Pipeline Current Path: %s" % str(os.getcwd()))
                fileObject, pathname, description = imp.find_module(uniqueColumnTransformName, \
                                                                    ['./lepipeline.tar.gz', './configurablepipelinetransformsfromfile'])
                columnTransformObject[self.loadedModuleKey] = imp.load_module(uniqueColumnTransformName, fileObject, pathname, description)
                logger.info("Loaded %s for Configurable Pipeline " % uniqueColumnTransformName) 

                mainClassName = value[self.mainClassNameKey]
                args = []
                namedParameterList = value[self.namedParameterListToInitKey]
                kwargs = self.buildKwArgs(namedParameterList = namedParameterList, \
                                          stringColumns = stringColumns, \
                                          categoricalColumns=categoricalColumns, \
                                          continuousColumns=continuousColumns, \
                                          targetColumn=targetColumn, \
                                          columnsToTransform=columnsToTransform)
                loadedObject = getattr(columnTransformObject[self.loadedModuleKey], mainClassName)(*args, **kwargs)
                columnTransformObject[self.loadedObjectKey] = loadedObject

                functionName = value[self.mainMethodNameKey]
                loadedModule = getattr(loadedObject, functionName)(*args, **kwargs)
                columnTransformObject[self.resultOfCallingMainMethodNameKey] = loadedModule

                pipelineAsArrayOfTransformClasses.append(loadedObject)

            logger.info("Finished Loading Configurable Pipeline")

            return pipelineAsArrayOfTransformClasses
        except Exception as e:
            logger.exception("Caught Exception while building Configurable Pipeline %s" % str(e))
            return None

    def buildKwArgs(self, namedParameterList=None, stringColumns = None, categoricalColumns=None, continuousColumns=None, targetColumn=None, columnsToTransform=None):
        kwargs = {}
        try:
            value = None

            for namedArgument, namedArgumentDataType in namedParameterList.iteritems():
                if namedArgumentDataType.lower() == "orderedDictContinuousColumns".lower():
                    if continuousColumns is None:
                        value = None
                    else:
                        value = OrderedDict(continuousColumns)
                elif namedArgumentDataType.lower() == "emptyDictionary".lower():
                    value = {}
                elif namedArgumentDataType.lower() == "categoricalColumns".lower():
                    value = categoricalColumns
                elif namedArgumentDataType.lower() == "emptyList".lower():
                    value = []
                elif namedArgumentDataType.lower() == "targetColumn".lower():
                    value = targetColumn
                elif namedArgumentDataType.lower() == "numericalColumn".lower():
                    value = continuousColumns
                elif namedArgumentDataType.lower() == "columnsToTransform".lower():
                    if categoricalColumns is not None and stringColumns is not None:
                        value = set(stringColumns - set(categoricalColumns.keys()))
                    else:
                        value = None
                else:
                    logger.error("Unknown ColumnType: %s. Assigning List as DataType" % namedArgument)
                    value = []

                if namedArgument not in kwargs:
                    kwargs[namedArgument] = value
                else:
                    logger.warning("Warning: NamedArgument %s has already been assigned to kwargs" % namedArgument)

            return kwargs
        except Exception:
            logger.exception("Caught Exception while building KWArgs for Configurable Pipeline")
            raise