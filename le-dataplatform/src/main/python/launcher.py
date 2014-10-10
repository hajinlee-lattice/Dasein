import logging
import os
import pwd
import string
import sys
from urlparse import urlparse

from leframework.argumentparser import ArgumentParser
from leframework.executors.learningexecutor import LearningExecutor
from leframework.webhdfs import WebHDFS

logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='launcher')

class Launcher(object):

    def __init__(self, modelFileName, propertyFileName=None):
        logger.info("Model file name = %s" % modelFileName)
        self.parser = ArgumentParser(modelFileName, propertyFileName)
        self.training = {}
        self.testing = {}

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

    def __validateEnv(self):
        self.__validateEnvVariable('SHDP_HD_FSWEB')
        self.__validateEnvVariable('CONTAINER_ID')
        
    def __validateParameters(self, schema):
        self.__validateSchemaParam(schema, "training_data")
        self.__validateSchemaParam(schema, "test_data")
        self.__validateSchemaParam(schema, "python_script")
        self.__validateSchemaParam(schema, "model_data_dir")

    def getParser(self):
        return self.parser
    
    def getClassifier(self):
        return self.clf;

    def execute(self, writeToHdfs, validateEnv=True, postProcessClf=True):
        parser = self.parser
        schema = parser.getSchema()

        # Fail fast if required parameters are not set
        if validateEnv:
            self.__validateEnv()
        self.__validateParameters(schema)

        # Extract data and scripts for execution
        self.training = parser.createList(self.stripPath(schema["training_data"]))
        self.test = parser.createList(self.stripPath(schema["test_data"]))
        script = self.stripPath(schema["python_script"])

        # Create directory for model result
        modelLocalDir = os.getcwd() + "/results/"
        
        if not os.path.exists(modelLocalDir):
            os.mkdir(modelLocalDir)

        # Get algorithm properties
        algorithmProperties = parser.getAlgorithmProperties()

        # Execute the packaged script from the client and get the returned file
        # that contains the generated model data
        execfile(script, globals())

        executor = LearningExecutor(parser.getRuntimeProperties())
        if 'getExecutor' in globals():
            executor = globals()["getExecutor"]()

        # Get hdfs model dir
        modelHdfsDir = executor.getModelDirPath(schema)

        params = dict()
        params["modelLocalDir"] = modelLocalDir
        params["modelHdfsDir"] = modelHdfsDir
        params["training"] = self.training
        params["test"] = self.test
        params["schema"] = schema
        params["parser"] = parser
        params["pipelineScript"] = self.stripPath(schema["python_pipeline_script"])
        schema["python_pipeline_script"] = params["pipelineScript"]
        schema["python_pipeline_lib"] = self.stripPath(schema["python_pipeline_lib"])

        (self.training, self.test, metadata) = executor.transformData(params)

        params["training"] = self.training
        params["test"] = self.test
        params["metadata"] = metadata

        # Passes runtime properties to report progress
        # training and testing data passed in as Pandas DataFrame
        self.clf = globals()["train"](self.training, self.test, schema, modelLocalDir, algorithmProperties, parser.getRuntimeProperties())

        if postProcessClf:
            executor.postProcessClassifier(self.clf, params)

        if writeToHdfs:
            # Create webhdfs instance for writing to hdfs
            webHdfsHostPort = urlparse(os.environ['SHDP_HD_FSWEB'])
            hdfs = WebHDFS(webHdfsHostPort.hostname, webHdfsHostPort.port, pwd.getpwuid(os.getuid())[0])
            hdfs.mkdir(modelHdfsDir)

            # Copy the model data files from local to hdfs
            (_, _, filenames) = os.walk(modelLocalDir).next()
            for filename in filenames:
                if executor.accept(filename) is False:
                    continue
                hdfs.copyFromLocal(modelLocalDir + filename, "%s/%s" % (modelHdfsDir, filename))
                if filename == "model.json":
                    modelName = parser.getSchema()["name"]
                    self.__publishToConsumer(hdfs, modelLocalDir + filename, modelHdfsDir, "BARD", modelName)

    def __publishToConsumer(self, hdfs, modelLocalPath, modelHdfsDir, consumer, modelName):
        # Id length cap by Bard, 120 for release
        modelIdLengthLimit = 45 
        tokens = string.split(modelHdfsDir, "/")
        modelToConsumerHdfsPath = ""
        for index in range(0, 5):
            modelToConsumerHdfsPath = modelToConsumerHdfsPath + tokens[index] + "/"

        modelId = tokens[7] + "-" + modelName 
        if len(modelId) > modelIdLengthLimit:
            modelId = modelId[:modelIdLengthLimit]
        modelToConsumerHdfsPath = modelToConsumerHdfsPath + consumer + "/" + modelId
        hdfs.rmdir(modelToConsumerHdfsPath + "/1.json")
        hdfs.copyFromLocal(modelLocalPath, "%s/%s" % (modelToConsumerHdfsPath, "1.json"))
        

def traverse(directory):
    (dirpath, directories, filenames) = os.walk(directory).next()
    
    for filename in filenames:
        logger.info(dirpath + "/" + filename)
    
    for d in directories:
        traverse(dirpath + "/" + d)

if __name__ == "__main__":
    """
    Transform the inputs into python objects and invoke user python script.
    
    Arguments:
    sys.argv[1] -- schema json file
    sys.argv[2] -- runtime properties file
    """

    logger.info("Python script launched with arguments: " + str(sys.argv[1:]))
    if len(sys.argv) != 3:
        logger.error("Argument length is :" + str(len(sys.argv)) + " which should be three.")

    logger.info("Files local to container")
    traverse(os.environ["PWD"])

    l = Launcher(sys.argv[1], sys.argv[2])
    l.execute(True)

