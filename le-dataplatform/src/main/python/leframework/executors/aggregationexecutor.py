import logging
import os
import shutil
import fastavro as avro
from pandas import Series

from leframework.codestyle import overrides
from leframework.executor import Executor
from leframework.model.statemachine import StateMachine
from leframework.model.states.averageprobabilitygenerator import AverageProbabilityGenerator
from leframework.model.states.normalizationgenerator import NormalizationGenerator
from leframework.model.states.bucketgenerator import BucketGenerator
from leframework.model.states.calibrationgenerator import CalibrationGenerator
from leframework.model.states.columnmetadatagenerator import ColumnMetadataGenerator
from leframework.model.states.datacompositiongenerator import DataCompositionGenerator
from leframework.model.states.enhancedsummarygenerator import EnhancedSummaryGenerator
from leframework.model.states.finalize import Finalize
from leframework.model.states.initialize import Initialize
from leframework.model.states.modeldetailgenerator import ModelDetailGenerator
from leframework.model.states.modelgenerator import ModelGenerator
from leframework.model.states.namegenerator import NameGenerator
from leframework.model.states.percentilebucketgenerator import PercentileBucketGenerator
from leframework.model.states.pmmlmodelgenerator import PMMLModelGenerator
from leframework.model.states.predictorgenerator import PredictorGenerator
from leframework.model.states.provenancegenerator import ProvenanceGenerator
from leframework.model.states.rocgenerator import ROCGenerator
from leframework.model.states.samplegenerator import SampleGenerator
from leframework.model.states.scorederivationgenerator import ScoreDerivationGenerator
from leframework.model.states.segmentationgenerator import SegmentationGenerator
from leframework.model.states.summarygenerator import SummaryGenerator

logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='aggregationexecutor')


class AggregationExecutor(Executor):

    def __init__(self, runtimeProperties=None):
        if runtimeProperties is None:
            logger.warn("No runtime properties available")
            self.amHost = None
            self.amPort = 0
        else:
            self.amHost = runtimeProperties["host"]
            self.amPort = int(runtimeProperties["port"])

    def __setupJsonGenerationStateMachine(self):
        stateMachine = StateMachine(self.amHost, self.amPort)
        stateMachine.addState(Initialize(), 1)  
        stateMachine.addState(NormalizationGenerator(), 2)  
        stateMachine.addState(CalibrationGenerator(), 5)
        stateMachine.addState(AverageProbabilityGenerator(), 3)
        stateMachine.addState(BucketGenerator(), 4)
        stateMachine.addState(ColumnMetadataGenerator(), 6)
        stateMachine.addState(ModelGenerator(), 7)
        stateMachine.addState(PMMLModelGenerator(), 8)
        stateMachine.addState(ROCGenerator(), 9)
        stateMachine.addState(ProvenanceGenerator(), 10)
        stateMachine.addState(PredictorGenerator(), 11)
        stateMachine.addState(ModelDetailGenerator(), 12)
        stateMachine.addState(SegmentationGenerator(), 13)
        stateMachine.addState(SummaryGenerator(), 14)
        stateMachine.addState(NameGenerator(), 15)
        stateMachine.addState(PercentileBucketGenerator(), 16)
        stateMachine.addState(SampleGenerator(), 17)
        stateMachine.addState(DataCompositionGenerator(), 18)
        stateMachine.addState(ScoreDerivationGenerator(), 19)
        stateMachine.addState(EnhancedSummaryGenerator(), 20)
        stateMachine.addState(Finalize(), 21)
        return stateMachine

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

    @overrides(Executor)
    def parseData(self, parser, trainingFile, testFile, postProcessClf):
        test = parser.createList(testFile, postProcessClf)
        return (None, test)

    @overrides(Executor)
    def writeToHdfs(self, hdfs, params):
        metadataFile = params["metadataFile"]
        modelLocalDir = params["modelLocalDir"]
        modelEnhancementsLocalDir = params["modelEnhancementsLocalDir"]
        modelEnhancementsHdfsDir = params["modelEnhancementsHdfsDir"]

        hdfs.copyToLocal(params["schema"]["diagnostics_path"] + "diagnostics.json", modelLocalDir + "diagnostics.json")
        if os.path.exists(metadataFile):
            shutil.copy2(metadataFile, modelLocalDir + "metadata.avsc")

        # Copy the enhanced model data files from local to hdfs
        hdfs.mkdir(modelEnhancementsHdfsDir)
        (_, _, filenames) = os.walk(modelEnhancementsLocalDir).next()
        for filename in filter(lambda e: self.accept(e), filenames):
            hdfs.copyFromLocal(modelEnhancementsLocalDir + filename, "%s%s" % (modelEnhancementsHdfsDir, filename))

        super(AggregationExecutor, self).writeToHdfs(hdfs, params)

    @overrides(Executor)
    def transformData(self, params):
        test = params["test"]
        schema = params["schema"]
        test[schema["reserved"]["training"]].update(Series([False] * test.shape[0]))
        params["allDataPreTransform"] = test
        
        metadata = self.retrieveMetadata(params["schema"]["data_profile"], params["parser"].isDepivoted())
        stringColumns = params["parser"].getStringColumns()

        # Execute the packaged script from the client and get the returned file
        # that contains the generated data pipeline
        script = params["pipelineScript"]
        execfile(script, globals())

        # Transform the categorical values in the metadata file into numerical values
        globals()["encodeCategoricalColumnsForMetadata"](metadata[0])

        # Create the data pipeline
        pipeline = globals()["setupPipeline"](metadata[0], stringColumns, params["parser"].target)
        params["pipeline"] = pipeline

        test = pipeline.predict(test)
        params["allDataPostTransform"] = test
        return (None, test, metadata)

    @overrides(Executor)
    def postProcessClassifier(self, clf, params):
        if clf != None:
            stateMachine = self.__setupJsonGenerationStateMachine()
            parser = params["parser"]
            mediator = stateMachine.getMediator()
            mediator.clf = clf
            mediator.modelLocalDir = params["modelLocalDir"]
            mediator.modelEnhancementsLocalDir = params["modelEnhancementsLocalDir"]
            mediator.modelHdfsDir = params["modelHdfsDir"]
            mediator.allDataPreTransform = params["allDataPreTransform"]
            mediator.allDataPostTransform = params["allDataPostTransform"]
            mediator.data = params["test"]
            mediator.schema = params["schema"]
            mediator.pipeline = params["pipeline"]
            mediator.depivoted = parser.isDepivoted()
            mediator.provenanceProperties = parser.getProvenanceProperties()
            mediator.metadata = params["metadata"]
            mediator.templateVersion = parser.templateVersion
            mediator.messages = []
            stateMachine.run()
        else:
            logger.warn("Generated classifier is null.")

    @overrides(Executor)
    def getModelDirPath(self, schema):
        if "CONTAINER_ID" in os.environ:
            appIdList = os.environ['CONTAINER_ID'].split("_")[1:3]
            modelDirPath = "%s/%s" % (schema["model_data_dir"], "_".join(appIdList))
            return modelDirPath
        else:
            return ""

    @overrides(Executor)
    def accept(self, filename):
        badSuffixes = [".p", ".dot", ".gz"]

        for badSuffix in badSuffixes:
            if filename.endswith(badSuffix):
                return False
        return True