import logging
import os
import pwd
import sys
import string
from urlparse import urlparse

from leframework.argumentparser import ArgumentParser
from leframework.executors.learningexecutor import LearningExecutor
from leframework.webhdfs import WebHDFS



logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='launcher')

class Launcher(object):
    
    def __init__(self, modelFileName):
        self.parser = ArgumentParser(modelFileName)
    
    def stripPath(self, fileName):
        return fileName[fileName.rfind('/') + 1:len(fileName)]

    def __validateEnvVariable(self, variable):
        try:
            os.environ[variable]
        except KeyError:
            raise Exception("%s environment variable not set." % (variable))

    def __validateSchemaParam(self, schema, param):
        try:
            schema[param]
        except KeyError:
            raise Exception("%s not set in job metadata." % (param))
    
    def __validateEnvAndParameters(self, schema):
        self.__validateEnvVariable('SHDP_HD_FSWEB')
        self.__validateEnvVariable('CONTAINER_ID')
        self.__validateSchemaParam(schema, "training_data")
        self.__validateSchemaParam(schema, "test_data")
        self.__validateSchemaParam(schema, "python_script")
        self.__validateSchemaParam(schema, "model_data_dir")
    
    def execute(self, writeToHdfs):
        parser = self.parser
        schema = parser.getSchema()
    
        # Fail fast if required parameters are not set
        self.__validateEnvAndParameters(schema)
    
        # Extract data and scripts for execution
        training = parser.createList(self.stripPath(schema["training_data"]))
        test = parser.createList(self.stripPath(schema["test_data"]))
        script = self.stripPath(schema["python_script"])

        # Create directory for model result
        modelLocalDir = os.getcwd() + "/results/"
        os.mkdir(modelLocalDir)
        
        # Get algorithm properties
        algorithmProperties = parser.getAlgorithmProperties()

        # Execute the packaged script from the client and get the returned file
        # that contains the generated model data
        execfile(script, globals())
        
        executor = LearningExecutor()
        if 'getExecutor' in globals():
            executor = globals()['getExecutor']()

        # Get hdfs model dir
        modelHdfsDir = executor.getModelDirPath(schema)

        params = dict()
        params["modelLocalDir"] = modelLocalDir
        params["modelHdfsDir"] = modelHdfsDir
        params["training"] = training
        params["test"] = test
        params["schema"] = schema
        params["parser"] = parser
            
        (training, test) = executor.transformData(params)

        params["training"] = training
        params["test"] = test
        
        clf = globals()['train'](training, test, schema, modelLocalDir, algorithmProperties)
        
        executor.postProcessClassifier(clf, params)

        if writeToHdfs:
            # Create webhdfs instance for writing to hdfs
            webHdfsHostPort = urlparse(os.environ['SHDP_HD_FSWEB'])
            hdfs = WebHDFS(webHdfsHostPort.hostname, webHdfsHostPort.port, pwd.getpwuid(os.getuid())[0])
            hdfs.mkdir(modelHdfsDir)

            # Copy the model data files from local to hdfs
            (_, _, filenames) = os.walk(modelLocalDir).next()
            for filename in filenames:
                hdfs.copyFromLocal(modelLocalDir + filename, "%s/%s" % (modelHdfsDir, filename))
                if filename == "model.json":
                    modelName = parser.getSchema()["name"]
                    self.__publishToConsumer(hdfs, modelLocalDir + filename, modelHdfsDir, "BARD", modelName)
    
    def __publishToConsumer(self, hdfs, modelLocalPath, modelHdfsDir, consumer, modelName):
        tokens = string.split(modelHdfsDir, "/")
        modelToConsumerHdfsPath = ""
        for index in range(0, 5):
            modelToConsumerHdfsPath = modelToConsumerHdfsPath + tokens[index] + "/"
            
        modelToConsumerHdfsPath = modelToConsumerHdfsPath + consumer + "/" + modelName
        hdfs.rmdir(modelToConsumerHdfsPath + "/1.json")
        hdfs.copyFromLocal(modelLocalPath, "%s/%s" % (modelToConsumerHdfsPath, "1.json"))

if __name__ == "__main__":
    """
    Transform the inputs into python objects and invoke user python script.
    
    Arguments:
    sys.argv[1] -- schema json file
    """
    l = Launcher(sys.argv[1])
    l.execute(True)
     
    
