import os
import fastavro as avro
from abc import ABCMeta, abstractmethod

class Executor(object):
    '''
    Base class for executing a data processing flow
    '''
    __metaclass__ = ABCMeta

    def __init__(self): pass

    @abstractmethod
    def parseData(self, parser, trainingFile, testFile, postProcessClf): pass

    def preTransformData(self, training, test, params):
        return (training, test)

    def postTransformData(self, training, test, params):
        return (training, test)

    @abstractmethod
    def transformData(self, params): pass

    @abstractmethod
    def postProcessClassifier(self, clf, params): pass

    def loadData(self):
        return True, True

    def writeToHdfs(self, hdfs, params):
        # Copy the model data files from local to hdfs
        modelLocalDir = params["modelLocalDir"]
        modelHdfsDir = params["modelHdfsDir"]
        hdfs.mkdir(modelHdfsDir)
        (_, _, filenames) = os.walk(modelLocalDir).next()
        for filename in filter(lambda e: self.accept(e), filenames):
            hdfs.copyFromLocal(modelLocalDir + filename, "%s%s" % (modelHdfsDir, filename))

    @abstractmethod
    def getModelDirPath(self, schema): pass

    @abstractmethod
    def accept(self, filename): pass

    def getModelDirByContainerId(self, schema):
        if "CONTAINER_ID" in os.environ:
            tokens = os.environ['CONTAINER_ID'].split("_")
            if(tokens[1].startswith("e")):
                appIdList = tokens[2:4]
            else:
                appIdList = tokens[1:3]
            modelDirPath = "%s/%s" % (schema["model_data_dir"], "_".join(appIdList))
            return modelDirPath
        else:
            return ""

    def retrieveMetadata(self, schema, depivoted):
        metadata = dict()
        realColNameToRecord = dict()

        if os.path.isfile(schema):
            with open(schema) as fp:
                reader = avro.reader(fp)
                for record in reader:
                    colname = record["barecolumnname"]
                    sqlcolname = ""
                    record["hashValue"] = None
                    if record["Dtype"] == "BND":
                        sqlcolname = colname + "_Continuous"  if depivoted else colname
                    elif depivoted:
                        sqlcolname = colname + "_" + record["columnvalue"]
                    else:
                        sqlcolname = colname
                        record["hashValue"] = record["columnvalue"]

                    if colname in metadata:
                        metadata[colname].append(record)
                    else:
                        metadata[colname] = [record]

                    realColNameToRecord[sqlcolname] = [record]
        return (metadata, realColNameToRecord)

    def createDataPipeline(self, params):
        metadata = self.retrieveMetadata(params["schema"]["data_profile"], params["parser"].isDepivoted())
        stringColumns = params["parser"].getStringColumns() - set(params["parser"].getKeys())
        pipelineDriver = params["schema"]["pipeline_driver"]
        pipelineLib = params["schema"]["python_pipeline_lib"]
        pipelineProps = params["schema"]["pipeline_properties"] if "pipeline_properties" in params["schema"] else ""

        # Execute the packaged script from the client and get the returned file
        # that contains the generated data pipeline
        script = params["pipelineScript"]
        execfile(script, globals())

        # Transform the categorical values in the metadata file into numerical values
        globals()["encodeCategoricalColumnsForMetadata"](metadata[0])

        pipelineParams = {}
        pipelineParams["schema"] = params["schema"]
        pipelineParams["idColumn"] = params["idColumn"]
        # Create the data pipeline
        pipeline, scoringPipeline = globals()["setupPipeline"](pipelineDriver, \
                                                               pipelineLib, \
                                                               metadata[0], \
                                                               stringColumns, \
                                                               params["parser"].target, \
                                                               pipelineParams, \
                                                               pipelineProps)
        params["pipeline"] = pipeline
        params["scoringPipeline"] = scoringPipeline

        return pipeline, metadata, pipelineParams
