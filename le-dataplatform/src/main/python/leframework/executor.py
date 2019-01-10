import fastavro as avro
import json
import logging
import os
import shutil
from abc import ABCMeta, abstractmethod

logger = logging.getLogger(__name__)

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

    def setupLocalDirectory(self, params):
        modelLocalDir = params["modelLocalDir"]
        modelEnhancementsLocalDir = params["modelEnhancementsLocalDir"]
        dataRulesLocalDir = params["dataRulesLocalDir"]
        pipelineLocalDir = params["pipelineLocalDir"]

        if not os.path.exists(modelLocalDir):
            os.mkdir(modelLocalDir)

        if not os.path.exists(modelEnhancementsLocalDir):
            os.mkdir(modelEnhancementsLocalDir)

        if not os.path.exists(pipelineLocalDir):
            os.mkdir(pipelineLocalDir)

        if not os.path.exists(dataRulesLocalDir):
            os.mkdir(dataRulesLocalDir)

    def writeToHdfs(self, hdfs, params):
        # Copy the model data files from local to hdfs
        modelLocalDir = params["modelLocalDir"]
        modelHdfsDir = params["modelHdfsDir"]
        metadataFile = params["metadataFile"]
        modelEnhancementsLocalDir = params["modelEnhancementsLocalDir"]
        pipelineLocalDir = params["pipelineLocalDir"]
        dataRulesLocalDir = params["dataRulesLocalDir"]

        # Copy the model data files from local to hdfs
        hdfs.mkdir(modelHdfsDir)
        if not os.path.exists(modelLocalDir + "diagnostics.json"):
            hdfs.copyToLocal(params["schema"]["diagnostics_path"] + "diagnostics.json", modelLocalDir + "diagnostics.json")
            self.mergeDiagnostics(modelLocalDir + "diagnostics.json", params)
            if os.path.exists(metadataFile):
                shutil.copy2(metadataFile, modelLocalDir + "metadata.avsc")
        (_, _, filenames) = os.walk(modelLocalDir).next()
        for filename in filter(lambda e: self.accept(e), filenames):
            hdfs.copyFromLocal(modelLocalDir + filename, "%s%s" % (modelHdfsDir, filename))

        # Copy the enhanced model data files from local to hdfs
        # Get hdfs model enhancements dir
        modelEnhancementsHdfsDir = modelHdfsDir + "enhancements/"

        hdfs.mkdir(modelEnhancementsHdfsDir)
        (_, _, filenames) = os.walk(modelEnhancementsLocalDir).next()
        for filename in filter(lambda e: self.accept(e), filenames):
            hdfs.copyFromLocal(modelEnhancementsLocalDir + filename, "%s%s" % (modelEnhancementsHdfsDir, filename))

        # Get hdfs pipeline dir
        pipelineHdfsDir = modelHdfsDir + "pipeline/"

        hdfs.mkdir(pipelineHdfsDir)
        (_, _, filenames) = os.walk(pipelineLocalDir).next()
        for filename in filter(lambda e: self.accept(e), filenames):
            hdfs.copyFromLocal(pipelineLocalDir + filename, "%s%s" % (pipelineHdfsDir, filename))

        # Copy the data rules files from local to hdfs
        dataRulesHdfsDir = modelHdfsDir + "datarules/"

        hdfs.mkdir(dataRulesHdfsDir)
        (_, _, filenames) = os.walk(dataRulesLocalDir).next()
        for filename in filter(lambda e: self.accept(e), filenames):
            hdfs.copyFromLocal(dataRulesLocalDir + filename, "%s%s" % (dataRulesHdfsDir, filename))

    def mergeDiagnostics(self, diagnostics, params):
        if 'pipelineMediator' not in params:
            return
        pipelineMediator = params['pipelineMediator']
        if pipelineMediator == None or ('REMOVEDCOLUMNS' not in pipelineMediator and 'ADDEDCOLUMNS' not in pipelineMediator):
            return

        with open(diagnostics, "r") as fp:
            fp_content = fp.read()

        if len(fp_content) > 0:
            diagnosticsJson = json.loads(fp_content)
            if 'REMOVEDCOLUMNS' in pipelineMediator:
                diagnosticsJson['Summary']['RemovedColumnsInPipeline'] = pipelineMediator["REMOVEDCOLUMNS"]
            if 'ADDEDCOLUMNS' in pipelineMediator:
                diagnosticsJson['Summary']['AddedColumnsInPipeline'] = pipelineMediator["ADDEDCOLUMNS"]
            with open(diagnostics, "wb") as fout:
                json.dump(diagnosticsJson, fout)
        else:
            logger.info("There is not diagnostics file to merge.")


    @abstractmethod
    def getModelDirPath(self, schema): pass

    @abstractmethod
    def accept(self, filename): pass

    def getModelDirByContainerId(self, schema):
        if "AWS_BATCH_JOB_ID" in os.environ:
            tokens = os.environ['AWS_BATCH_JOB_ID'].split("_")
            appIdList = tokens[1:3]
            modelDirPath = "%s/%s" % (schema["model_data_dir"], "_".join(appIdList))
            return modelDirPath
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

        if os.path.isfile(schema) and schema.endswith(".avro"):
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
        (profile, profileByDepivotColName) = self.retrieveMetadata(params["schema"]["data_profile"], params["parser"].isDepivoted())
        columnMetadata = params["schema"]["config_metadata"]["Metadata"] if params["schema"]["config_metadata"] is not None else None
        stringColumns = params["parser"].getStringColumns() - set(params["parser"].getKeys())
        pipelineDriver = params["schema"]["pipeline_driver"]
        pipelineLib = params["schema"]["python_pipeline_lib"]
        pipelineProps = params["schema"]["pipeline_properties"] if "pipeline_properties" in params["schema"] else ""

        # Execute the packaged script from the client and get the returned file
        # that contains the generated data pipeline
        script = params["pipelineScript"]
        execfile(script, globals())

        # Transform the categorical values in the metadata file into numerical values
        globals()["encodeCategoricalColumnsForMetadata"](profile)

        pipelineParams = {}
        pipelineParams["schema"] = params["schema"]
        pipelineParams["idColumn"] = params["idColumn"]
        # Create the data pipeline
        pipeline, scoringPipeline = globals()["setupPipeline"](pipelineDriver, \
                                                               pipelineLib, \
                                                               profile, \
                                                               columnMetadata, \
                                                               stringColumns, \
                                                               params["parser"].target, \
                                                               pipelineParams, \
                                                               pipelineProps)
        params["pipeline"] = pipeline
        params["scoringPipeline"] = scoringPipeline

        return pipeline, (profile, profileByDepivotColName), pipelineParams
