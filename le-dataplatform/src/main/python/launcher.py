import logging
import os
from pandas import Series
from pandas.core.frame import DataFrame
import pwd
import shutil
import sys
from urlparse import urlparse
 
from leframework.argumentparser import ArgumentParser
from leframework.executors.learningexecutor import LearningExecutor
from leframework.progressreporter import ProgressReporter
from leframework.webhdfs import WebHDFS
 
 
logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='launcher')
 
class Launcher(object):
 
    def __init__(self, modelFileName, propertyFileName=None):
        logger.info("Model file name = %s" % modelFileName)
        self.parser = ArgumentParser(modelFileName, propertyFileName)
        self.training = {}
        self.test = {}
 
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
     
    def getNewFeatures(self, allColumnsPreTransform, allColumnsPostTransform):
        return list(set(allColumnsPostTransform) - set(allColumnsPreTransform))
 
    def updatedFields(self, fields, newFeatures, trainingData, testData):
        chosenDataset = trainingData
        if isinstance(testData, DataFrame):
            chosenDataset = testData
             
        fieldDicts = fields
        sqlType = 8
        fieldType = 'double'
         
        for feature in newFeatures:
            if chosenDataset[feature].dtype == 'string':
                sqlType = -9
                fieldType = 'string'
                 
            fieldDicts.append({'columnName' : feature, 'sqlType': str(sqlType), 'type': [str(fieldType), 'null'], 'name': feature})
        return fieldDicts
     
    def getColumnNames(self, trainingData, testData):
        chosenDataset = trainingData
        if isinstance(testData, DataFrame):
            chosenDataset = testData
             
        return chosenDataset.columns.values
     
    def execute(self, writeToHdfs, validateEnv=True, postProcessClf=True):
        parser = self.parser
        schema = parser.getSchema()
 
        # Fail fast if required parameters are not set
        if validateEnv:
            self.__validateEnv()
        self.__validateParameters(schema)
 
        # Extract data and scripts for execution
        runtimeProperties = parser.getRuntimeProperties()
        if runtimeProperties is not None:
            progressReporter = ProgressReporter(runtimeProperties["host"], int(runtimeProperties["port"]))
        else:
            progressReporter = ProgressReporter(None, 0)
        progressReporter.setTotalState(2)
         
        metadataFile = self.stripPath(schema["config_metadata"])
        progressReporter.nextStateForPreStateMachine(0, 0.1, 1)
        script = self.stripPath(schema["python_script"])
        progressReporter.nextStateForPreStateMachine(0, 0.1, 2)
         
        # Create directory for model result
        modelLocalDir = os.getcwd() + "/results/"
         
        if not os.path.exists(modelLocalDir):
            os.mkdir(modelLocalDir)
 
        # Create directory for model enhancements
        modelEnhancementsLocalDir = modelLocalDir + "enhancements/"
 
        if not os.path.exists(modelEnhancementsLocalDir):
            os.mkdir(modelEnhancementsLocalDir)
 
        # Get algorithm properties
        algorithmProperties = parser.getAlgorithmProperties()
 
        # Execute the packaged script from the client and get the returned file
        # that contains the generated model data
        execfile(script, globals())
 
        executor = LearningExecutor(runtimeProperties)
        if 'getExecutor' in globals():
            executor = globals()["getExecutor"]()
 
        # Get hdfs model dir
        modelHdfsDir = executor.getModelDirPath(schema)
        if not modelHdfsDir.endswith("/"): modelHdfsDir += "/"
 
        # Get hdfs model enhancements dir
        modelEnhancementsHdfsDir = modelHdfsDir + "enhancements/"
 
        params = dict()
        params["modelLocalDir"] = modelLocalDir
        params["modelEnhancementsLocalDir"] = modelEnhancementsLocalDir
        params["modelHdfsDir"] = modelHdfsDir
        params["schema"] = schema
        params["parser"] = parser
        params["pipelineScript"] = self.stripPath(schema["python_pipeline_script"])
        schema["python_pipeline_script"] = params["pipelineScript"]
        schema["python_pipeline_lib"] = self.stripPath(schema["python_pipeline_lib"])
 
        loadTrainingData, loadTestData = executor.loadData()
        if loadTrainingData:
            self.training = parser.createList(self.stripPath(schema["training_data"]), postProcessClf)
 
        if loadTestData:
            self.test = parser.createList(self.stripPath(schema["test_data"]), postProcessClf)
 
        params["training"] = self.training
        params["test"] = self.test
 
        allColumnsPreTransform = self.getColumnNames(self.training, self.test) 
         
        # Processing steps in learning executor
        if isinstance(executor, LearningExecutor):
            self.training[schema["reserved"]["training"]].update(Series([True] * self.training.shape[0]))
            self.test[schema["reserved"]["training"]].update(Series([False] * self.test.shape[0]))
            params["allDataPreTransform"] = DataFrame.append(self.training, self.test)
            (self.training, self.test, metadata) = executor.transformData(params)
            params["allDataPostTransform"] = DataFrame.append(self.training, self.test)
        # Processing steps in all other executors (profiling, aggregation, and parallel learning executors)
        else:
            (self.training, self.test, metadata) = executor.transformData(params)
 
        params["readouts"] = parser.readouts
        params["training"] = self.training
        params["test"] = self.test
        params["metadata"] = metadata
 
        allColumnsPostTransform = self.getColumnNames(self.training, self.test) 
        newFeatures = self.getNewFeatures(allColumnsPreTransform, allColumnsPostTransform)
        if len(newFeatures) > 0:
            params["schema"]["features"] = params["schema"]["features"] + newFeatures
            parser.fields = self.updatedFields(parser.fields, newFeatures, self.training, self.test)
 
        # Passes runtime properties to report progress
        # training and testing data passed in as Pandas DataFrame
        self.clf = globals()["train"](self.training, self.test, schema, modelLocalDir, algorithmProperties, parser.getRuntimeProperties(), params)
 
        if postProcessClf:
            executor.postProcessClassifier(self.clf, params)
 
        if writeToHdfs:
            # Create webhdfs instance for writing to hdfs
            webHdfsHostPort = urlparse(os.environ['SHDP_HD_FSWEB'])
            hdfs = WebHDFS(webHdfsHostPort.hostname, webHdfsHostPort.port, pwd.getpwuid(os.getuid())[0])
 
            # Copy the model data files from local to hdfs
            hdfs.mkdir(modelHdfsDir)
            if not os.path.exists(modelLocalDir + "diagnostics.json"):
                hdfs.copyToLocal(params["schema"]["diagnostics_path"] + "diagnostics.json", modelLocalDir + "diagnostics.json")
                if os.path.exists(metadataFile):
                    shutil.copy2(metadataFile, modelLocalDir + "metadata.avsc")
            (_, _, filenames) = os.walk(modelLocalDir).next()
            for filename in filter(lambda e: executor.accept(e), filenames):
                hdfs.copyFromLocal(modelLocalDir + filename, "%s%s" % (modelHdfsDir, filename))
 
            # Copy the enhanced model data files from local to hdfs?
            if self.clf != None:
                hdfs.mkdir(modelEnhancementsHdfsDir)
                (_, _, filenames) = os.walk(modelEnhancementsLocalDir).next()
                for filename in filter(lambda e: executor.accept(e), filenames):
                    hdfs.copyFromLocal(modelEnhancementsLocalDir + filename, "%s%s" % (modelEnhancementsHdfsDir, filename))
 
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