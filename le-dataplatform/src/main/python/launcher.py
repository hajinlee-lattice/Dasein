import logging
import os
import pwd
import sys
from leframework.argumentparser import ArgumentParser
from leframework.model.statemachine import StateMachine
from leframework.model.states.bucketgenerator import BucketGenerator
from leframework.model.states.calibrationgenerator import CalibrationGenerator
from leframework.model.states.finalize import Finalize
from leframework.model.states.initialize import Initialize
from leframework.model.states.modelgenerator import ModelGenerator
from leframework.model.states.summarygenerator import SummaryGenerator
from leframework.webhdfs import WebHDFS
from urlparse import urlparse

logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='launcher')

class Launcher(object):
    
    def __init__(self, modelFileName):
        self.parser = ArgumentParser(modelFileName)
    
    def __stripPath(self, fileName):
        return fileName[fileName.rfind('/')+1:len(fileName)]

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
    
    def __getModelDirPath(self, schema):
        appIdList = os.environ['CONTAINER_ID'].split("_")[1:3]
        modelDirPath = "%s/%s" % (schema["model_data_dir"], "_".join(appIdList))
        return modelDirPath

    def __setupJsonGenerationStateMachine(self):
        stateMachine = StateMachine()
        stateMachine.addState(Initialize())
        stateMachine.addState(CalibrationGenerator())
        stateMachine.addState(BucketGenerator())
        stateMachine.addState(ModelGenerator())
        stateMachine.addState(SummaryGenerator())
        stateMachine.addState(Finalize())
        return stateMachine

    def populateSchemaWithMetadata(self, schema, parser):
        schema["featureIndex"] = parser.getFeatureTuple()
        schema["targetIndex"] = parser.getTargetIndex()
        schema["keyColIndex"] = parser.getKeyColumns()
        
    def execute(self, writeToHdfs):
        parser = self.parser
        schema = parser.getSchema()
    
        # Fail fast if required parameters are not set
        self.__validateEnvAndParameters(schema)
    
        # Extract data and scripts for execution
        training = parser.createList(self.__stripPath(schema["training_data"]))
        test = parser.createList(self.__stripPath(schema["test_data"]))
        script = self.__stripPath(schema["python_script"])
        self.populateSchemaWithMetadata(schema, parser)

        # Create directory for model result
        modelLocalDir = os.getcwd() + "/results/"
        os.mkdir(modelLocalDir)
        
        # Get hdfs model dir
        modelHdfsDir = self.__getModelDirPath(schema)
        # Get algorithm properties
        algorithmProperties = parser.getAlgorithmProperties()

        # Execute the packaged script from the client and get the returned file
        # that contains the generated model data
        execfile(script, globals())
        clf = globals()['train'](training, test, schema, modelLocalDir, algorithmProperties)

        if clf != None:
            stateMachine = self.__setupJsonGenerationStateMachine()
            mediator = stateMachine.getMediator()
            mediator.clf = clf
            mediator.modelLocalDir = modelLocalDir
            mediator.modelHdfsDir = modelHdfsDir
            mediator.data = test
            mediator.schema = schema
            stateMachine.run()
        else:
            logger.error("Generated classifier is null!")
    
        if writeToHdfs:
            # Create webhdfs instance for writing to hdfs
            webHdfsHostPort = urlparse(os.environ['SHDP_HD_FSWEB'])
            hdfs = WebHDFS(webHdfsHostPort.hostname, webHdfsHostPort.port, pwd.getpwuid(os.getuid())[0])
            hdfs.mkdir(modelHdfsDir)

            # Copy the model data files from local to hdfs
            (_, _, filenames) = os.walk(modelLocalDir).next()
            for filename in filenames:
                hdfs.copyFromLocal(modelLocalDir + filename, "%s/%s" % (modelHdfsDir, filename))


if __name__ == "__main__":
    """
    Transform the inputs into python objects and invoke user python script.
    
    Arguments:
    sys.argv[1] -- schema json file
    """
    l = Launcher(sys.argv[1])
    l.execute(True)
     
    
