'''
Contains all column tranformation functions, whether to apply to categorical or numerical columns
The actual function to apply are taken from model.json file
'''

from collections import OrderedDict
import imp
import json
import logging
import os
import pwd

from urlparse import urlparse
from leframework.webhdfs import WebHDFS

logger = logging.getLogger(name='columntransform')

class ColumnTransform(object):

    pipelineFileAsJson = ""
    columnTransformKey = "columnTransformFiles"

    uniqueColumnTransformKey = "Name"
    mainMethodNameKey = "MainMethodName"
    uniqueColumnTransformNameKey = "UniqueColumnTransformName"
    columnTransformFilePathKey = "ColumnTransformFilePath"
    loadedModuleKey = "LoadedModule"
    mainClassNameKey = "MainClassName"
    namedParameterListToInitKey = "NamedParameterListToInit"
    loadedObjectKey = "LoadedObject"
    resultOfCallingMainMethodNameKey = "ResultOfCallingMainMethodName"
    sortingKey = "KeyWhenSortingByAscending"

    def __init__(self, pathToPipelineFiles=None):
        if pathToPipelineFiles is not None:
            try:
                for pipelineFile in pathToPipelineFiles:
                    logger.info("Opening file %s." % pipelineFile)
                    logger.info(os.getcwd())
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
    
    def stripPath(self, fileName):
        return fileName[fileName.rfind('/') + 1:len(fileName)]

    def buildPipelineFromFile(self, pipelinePath="./lepipeline.tar.gz",
                                       stringColumns=None,
                                       categoricalColumns=None,
                                       continuousColumns=None,
                                       customerColumns=None,
                                       targetColumn=None,
                                       columnsToTransform=None,
                                       profile=None,
                                       allColumns=None,
                                       params=None):
        # Return Array that holds functions loaded from file
        pipelineAsArrayOfTransformClasses = []
        pipelineName = []
        defaultDisabledSteps = []
        
        try:
            jsonToProcess = self.pipelineFileAsJson;
            if jsonToProcess is None:
                logger.info("JsonToProcess is Empty. No transforms could be created.")
                return pipelineAsArrayOfTransformClasses

            if self.checkJsonForCorrectness(jsonToProcess) == False:
                logger.info("Couldn't validate JSON file %s for configurable pipeline." % jsonToProcess)
                return pipelineAsArrayOfTransformClasses

            webHdfsHostPort = urlparse(os.environ['SHDP_HD_FSWEB'])
            hdfs = WebHDFS(webHdfsHostPort.hostname, webHdfsHostPort.port, pwd.getpwuid(os.getuid())[0])

            for _, value in sorted(jsonToProcess[self.columnTransformKey].iteritems(), key=lambda item: item[1][self.sortingKey]):
                if "LoadFromHdfs" in value and value["LoadFromHdfs"] == True:
                    f = value["ColumnTransformFilePath"]
                    f = self.stripPath(f)
                    logger.info('Loading PipelineStep from HDFS: {0} --> {1}'.format(value["ColumnTransformFilePath"], '{0}/{1}'.format(pipelinePath, f)))
                    hdfs.copyToLocal(value["ColumnTransformFilePath"], '{0}/{1}'.format(pipelinePath, f))
                    rtsFile = value["RTSFilePath"] if "RTSFilePath" in value else None

                    if rtsFile is not None:
                        rtsFile = self.stripPath(rtsFile)
                        logger.info('Loading Python Transform from HDFS: {0} --> {1}'.format(value["RTSFilePath"], '{0}/{1}'.format(pipelinePath, rtsFile)))
                        hdfs.copyToLocal(value["RTSFilePath"], '{0}/{1}'.format(pipelinePath, rtsFile))

                uniqueColumnTransformName = value[self.uniqueColumnTransformKey]

                columnTransformObject = {}
                columnTransformObject[self.uniqueColumnTransformNameKey] = value[self.uniqueColumnTransformNameKey]

                sourceFilePathLowerCased = str(value[self.columnTransformFilePathKey]).lower()
                columnTransformObject[self.columnTransformFilePathKey] = sourceFilePathLowerCased

                logger.info("Configurable Pipeline Current Path: %s" % str(os.getcwd()))
                fileObject, pathname, description = imp.find_module(uniqueColumnTransformName, [pipelinePath, os.getcwd()])
                columnTransformObject[self.loadedModuleKey] = imp.load_module(uniqueColumnTransformName, fileObject, pathname, description)
                logger.info("Loaded %s for Configurable Pipeline " % uniqueColumnTransformName)

                mainClassName = value[self.mainClassNameKey]
                namedParameterList = value[self.namedParameterListToInitKey]
                kwargs = self.buildKwArgs(namedParameterList=namedParameterList, \
                                          stringColumns=stringColumns, \
                                          categoricalColumns=categoricalColumns, \
                                          continuousColumns=continuousColumns, \
                                          customerColumns=customerColumns, \
                                          targetColumn=targetColumn, \
                                          columnsToTransform=columnsToTransform, \
                                          profile=profile, \
                                          allColumns=allColumns, \
                                          params=params)
                argsCopy = dict(kwargs);
                if kwargs.has_key("enabled"):
                    del kwargs["enabled"]
                loadedObject = getattr(columnTransformObject[self.loadedModuleKey], mainClassName)(**kwargs)
                columnTransformObject[self.loadedObjectKey] = loadedObject

                loadedModule = getattr(loadedObject, "__init__")(**kwargs)
                columnTransformObject[self.resultOfCallingMainMethodNameKey] = loadedModule

                pipelineName.append(uniqueColumnTransformName)
                pipelineAsArrayOfTransformClasses.append(loadedObject)
                if argsCopy.has_key("enabled") and (argsCopy["enabled"].lower() == "false"):
                    logger.info("By default skip disabled step " + uniqueColumnTransformName)
                    defaultDisabledSteps.append(loadedObject)

            logger.info("Finished Loading Configurable Pipeline")

            return (pipelineName, pipelineAsArrayOfTransformClasses, defaultDisabledSteps)
        except Exception as e:
            logger.exception("Caught Exception while building Configurable Pipeline %s" % str(e))
            return None

    def buildKwArgs(self, namedParameterList=None,
                            stringColumns=None,
                            categoricalColumns=None,
                            continuousColumns=None,
                            customerColumns=None,
                            targetColumn=None,
                            columnsToTransform=None,
                            profile=None,
                            allColumns=None,
                            params=None):
        kwargs = {}
        try:
            for namedArgument, namedArgumentDataTypeOrValue in namedParameterList.iteritems():
                value = None

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
                elif namedArgumentDataTypeOrValue.lower() == "customerColumns".lower():
                    value = customerColumns
                elif namedArgumentDataTypeOrValue.lower() == "emptyList".lower():
                    value = []
                elif namedArgumentDataTypeOrValue.lower() == "targetColumn".lower():
                    value = targetColumn
                elif namedArgumentDataTypeOrValue.lower() == "numericalColumns".lower():
                    value = continuousColumns
                elif namedArgumentDataTypeOrValue.lower() == "dataprofile".lower():
                    value = profile
                elif namedArgumentDataTypeOrValue.lower() == "params".lower():
                    value = params
                elif namedArgumentDataTypeOrValue.lower() == "columnsToTransform".lower():
                    if categoricalColumns is not None and stringColumns is not None:
                        value = set(stringColumns - set(categoricalColumns.keys()))
                    else:
                        value = None
                elif namedArgumentDataTypeOrValue.lower() == "allColumns".lower():
                    if value is None:
                        value = {}
                    if categoricalColumns:
                        if type(categoricalColumns) is dict:
                            value = categoricalColumns.copy()
                        elif  isinstance(categoricalColumns, str) or isinstance(categoricalColumns, unicode):
                            value = {categoricalColumns : categoricalColumns}
                    if continuousColumns and continuousColumns.keys():
                        value.update(continuousColumns)
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
