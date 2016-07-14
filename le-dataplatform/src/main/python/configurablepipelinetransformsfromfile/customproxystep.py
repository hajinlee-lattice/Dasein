'''
Description:

    This step will serve as proxy to run a custom target step dynamically
'''
import os
import shutil
import pwd
import imp
from leframework.webhdfs import WebHDFS
from urlparse import urlparse
from pipelinefwk import PipelineStep
from pipelinefwk import get_logger


logger = get_logger("pipeline")

class CustomProxyStep(PipelineStep):

    def __init__(self, params, targetName, targetMainClassName, targetFilePath):
        self.params = params
        self.targetName = targetName
        self.targetMainClassName = targetMainClassName
        self.targetFilePath = targetFilePath
        self.targetObject = None
        if params:
            try:
                if self.__isBlank(self.targetName) or self.__isBlank(self.targetMainClassName) or self.__isBlank(self.targetFilePath):
                    logger.info('No target step is defined.')
                    return
                localTargetFilePath = params["schema"]["python_pipeline_lib"] + "/" + self.__stripPath(self.targetFilePath)
                localTempFile = os.getcwd() + "/" + self.__stripPath(self.targetFilePath)
                if os.path.exists(localTempFile):
                    shutil.copy2(localTempFile, localTargetFilePath)
                else:
                    if os.environ['SHDP_HD_FSWEB'] == None:
                        return
                    webHdfsHostPort = urlparse(os.environ['SHDP_HD_FSWEB'])
                    hdfs = WebHDFS(webHdfsHostPort.hostname, webHdfsHostPort.port, pwd.getpwuid(os.getuid())[0])
                    files = hdfs.listdir(self.targetFilePath)
                    if len(files) <= 0:
                        logger.info("custom target file %s does not exist." % (self.targetFilePath))
                        return
                    logger.info("Copying custom target file %s to local %s." % (self.targetFilePath, localTargetFilePath))
                    hdfs.copyToLocal(self.targetFilePath, localTargetFilePath)

                logger.info("Configurable Pipeline Current Path: %s" % str(os.getcwd()))
                paths = [params["schema"]["python_pipeline_lib"]]
                fileObject, pathname, description = imp.find_module(targetName, paths)
                loadedModule = imp.load_module(targetName, fileObject, pathname, description)
                kwargs = {'params' : params}
                self.targetObject = getattr(loadedModule, targetMainClassName)(**kwargs)
                logger.info('Successfully loaded CustomTargetStep for ' + self.targetFilePath)
                
            except Exception as e:
                self.targetObject = None
                logger.warn("Failed to load the custom target object in file %s, error: %s" % (targetFilePath, str(e)))

    def transform(self, dataFrame, configMetadata, test):
        if self.targetObject == None:
            logger.info('No custom target class was loaded.')
            return dataFrame
        logger.info('Starting to run CustomProxyStep for ' + self.targetFilePath)
        try:
            return self.targetObject.transform(dataFrame, configMetadata, test)
            logger.info('Successfully run CustomProxyStep for ' + self.targetFilePath)
        except Exception as e:
            logger.error("Failed to run custom target class in file %s, error: %s" % (self.targetFilePath, str(e)))
            
        return dataFrame
    
    def __isBlank(self, str):
        if str == None or str.strip() == '':
            return True
        return False

    def __stripPath(self, fileName):
        return fileName[fileName.rfind("/") + 1:len(fileName)]
