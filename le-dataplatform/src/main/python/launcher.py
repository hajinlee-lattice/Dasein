import logging
import os
import pwd
import sys
from pandas.core.frame import DataFrame
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
        return fileName[fileName.rfind("/") + 1:len(fileName)]

    def __validateEnvVariable(self, variable):
        try:
            logger.info("Validated env var %s=%s" % (variable, os.environ[variable]))
        except KeyError:
            raise Exception("%s environment variable not set." % (variable))

    def __validateSchemaParam(self, schema, param):
        try:
            schema[param]
        except KeyError:
            raise Exception("%s not set in job metadata." % (param))

    def __validateEnv(self):
        self.__validateEnvVariable("SHDP_HD_FSWEB")
#         self.__validateEnvVariable("CONTAINER_ID")
        self.__validateEnvVariable("DEBUG")

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

    def getRemovedFeatures(self, allColumnsPreTransform, allColumnsPostTransform):
        return list(set(allColumnsPreTransform) - set(allColumnsPostTransform))

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
            if "webhdfs" in runtimeProperties:
                os.environ["SHDP_HD_FSWEB"] = runtimeProperties["webhdfs"]
        else:
            progressReporter = ProgressReporter(None, 0)
        progressReporter.setTotalState(2)

        metadataFile = self.stripPath(schema["config_metadata"])
        progressReporter.nextStateForPreStateMachine(0, 0.1, 1)
        script = self.stripPath(schema["python_script"])
        progressReporter.nextStateForPreStateMachine(0, 0.1, 2)

        modelLocalDir = os.getcwd() + "/results/"
        modelEnhancementsLocalDir = modelLocalDir + "enhancements/"
        pipelineLocalDir = modelLocalDir + "pipeline/"
        dataRulesLocalDir = modelLocalDir + "datarules/"

        # Execute the packaged script from the client and get the returned file
        # that contains the generated model data
        execfile(script, globals())

        executor = LearningExecutor(runtimeProperties)
        if 'getExecutor' in globals():
            executor = globals()["getExecutor"](runtimeProperties)

        # Get hdfs model dir
        modelHdfsDir = executor.getModelDirPath(schema)
        if not modelHdfsDir.endswith("/"): modelHdfsDir += "/"

        params = dict()
        params["modelLocalDir"] = modelLocalDir
        params["modelEnhancementsLocalDir"] = modelEnhancementsLocalDir
        params["pipelineLocalDir"] = pipelineLocalDir
        params["dataRulesLocalDir"] = dataRulesLocalDir
        params["modelHdfsDir"] = modelHdfsDir
        params["metadataFile"] = metadataFile
        params["schema"] = schema
        params["parser"] = parser
        params["pipelineScript"] = self.stripPath(schema["python_pipeline_script"])
        schema["python_pipeline_script"] = params["pipelineScript"]
        schema["python_pipeline_lib"] = self.stripPath(schema["python_pipeline_lib"])

        executor.setupLocalDirectory(params)

        try:
            schema["pipeline_driver"] = self.stripPath(schema["pipeline_driver"])
        except Exception:
            logger.warn("Setting pipeline driver to pipeline.json")
            schema["pipeline_driver"] = "pipeline.json"

        loadTrainingData, loadTestData = executor.loadData()
        self.training = parser.createList(self.stripPath(schema["training_data"]), postProcessClf, loadTrainingData)
        self.test = parser.createList(self.stripPath(schema["test_data"]), postProcessClf, loadTestData)

        params["training"] = self.training
        params["test"] = self.test
        params["idColumn"] = self.selectIdColumn(self.training)


        allColumnsPreTransform = self.getColumnNames(self.training, self.test)
        (self.training, self.test) = executor.preTransformData(self.training, self.test, params)
        (self.training, self.test, metadata) = executor.transformData(params)
        (self.training, self.test) = executor.postTransformData(self.training, self.test, params)

        params["readouts"] = parser.readouts
        params["training"] = self.training
        params["test"] = self.test
        params["metadata"] = metadata

        allColumnsPostTransform = self.getColumnNames(self.training, self.test)
        newFeatures = self.getNewFeatures(allColumnsPreTransform, allColumnsPostTransform)
        removedFeatures = set(self.getRemovedFeatures(allColumnsPreTransform, allColumnsPostTransform))
        if len(newFeatures) > 0:
            params["schema"]["features"] = params["schema"]["features"] + newFeatures
            parser.fields = self.updatedFields(parser.fields, newFeatures, self.training, self.test)

        if len(removedFeatures) > 0:
            params["schema"]["features"] = [ x for x in params["schema"]["features"] if x not in removedFeatures ]


        if self.training is not None:
            dataFrameColumns = set(self.training.columns.values.tolist())
            schema["features"] = [x for x in schema["features"] if x in dataFrameColumns]

        # Passes runtime properties to report progress
        # training and testing data passed in as Pandas DataFrame
        # Get algorithm properties
        algorithmProperties = parser.getAlgorithmProperties()
        self.clf = globals()["train"](self.training,
                                      self.test,
                                      schema,
                                      modelLocalDir,
                                      algorithmProperties,
                                      parser.getRuntimeProperties(),
                                      params)

        if postProcessClf:
            executor.postProcessClassifier(self.clf, params)

        if writeToHdfs:
            # Create webhdfs instance for writing to hdfs
            webHdfsHostPort = urlparse(os.environ['SHDP_HD_FSWEB'])
            hdfs = WebHDFS(webHdfsHostPort.hostname, webHdfsHostPort.port, pwd.getpwuid(os.getuid())[0])

            executor.writeToHdfs(hdfs, params)


    def selectIdColumn(self, dataFrame):
        if dataFrame is None or hasattr(dataFrame, 'columns') == False:
            return None

        if "Id" in dataFrame.columns:
            return "Id"
        elif "LeadID" in dataFrame.columns:
            return "LeadID"
        elif "ExternalID" in dataFrame.columns:
            return "ExternalID"
        else:
            return None

def traverse(directory):
    for dirpath, sub_dirs, filenames in os.walk(directory):
        for filename in filenames:
            logger.info(dirpath + "/" + filename)

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
