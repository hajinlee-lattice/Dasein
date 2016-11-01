import logging
import os
import shutil
from pandas import Series
from pandas import DataFrame

from leframework.codestyle import overrides
from leframework.executor import Executor
from leframework.model.statemachine import StateMachine
from leframework.model.states.averageprobabilitygenerator import AverageProbabilityGenerator
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
from leframework.model.states.normalizationgenerator import NormalizationGenerator
from leframework.model.states.percentilebucketgenerator import PercentileBucketGenerator
from leframework.model.states.pmmlmodelgenerator import PMMLModelGenerator
from leframework.model.states.predictorgenerator import PredictorGenerator
from leframework.model.states.provenancegenerator import ProvenanceGenerator
from leframework.model.states.rocgenerator import ROCGenerator
from leframework.model.states.samplegenerator import SampleGenerator
from leframework.model.states.scorederivationgenerator import ScoreDerivationGenerator
from leframework.model.states.segmentationgenerator import SegmentationGenerator
from leframework.model.states.summarygenerator import SummaryGenerator
from leframework.model.states.crossvalidationgenerator import CrossValidationGenerator
from leframework.model.states.modelpredictorgenerator import ModelPredictorGenerator


logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='learningexecutor')


class LearningExecutor(Executor):
    '''
    Base executor for all machine learning modeling. It does basic data preparation before
    passing the data into the modeling.
    '''


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
        stateMachine.addState(CrossValidationGenerator(), 20)
        stateMachine.addState(EnhancedSummaryGenerator(), 21)
        stateMachine.addState(ModelPredictorGenerator(), 22)
        stateMachine.addState(Finalize(), 23)
        return stateMachine

    @overrides(Executor)
    def loadData(self):
        return True, True

    @overrides(Executor)
    def parseData(self, parser, trainingFile, testFile, postProcessClf):
        training = parser.createList(trainingFile, postProcessClf)
        test = parser.createList(testFile, postProcessClf)
        return (training, test)

    @overrides(Executor)
    def writeToHdfs(self, hdfs, params):
        super(LearningExecutor, self).writeToHdfs(hdfs, params)

    @overrides(Executor)
    def preTransformData(self, training, test, params):
        schema = params["schema"]
        if schema["reserved"] is not None:
            training[schema["reserved"]["training"]].update(Series([True] * training.shape[0]))
            test[schema["reserved"]["training"]].update(Series([False] * test.shape[0]))
        params["allDataPreTransform"] = DataFrame.append(training, test)
        return (training, test)

    @overrides(Executor)
    def postTransformData(self, training, test, params):
        params["allDataPostTransform"] = DataFrame.append(training, test)
        return (training, test)


    @overrides(Executor)
    def transformData(self, params):
        pipeline, metadata, pipelineParams = self.createDataPipeline(params)
        configMetadata = params["schema"]["config_metadata"]["Metadata"] if params["schema"]["config_metadata"] is not None else None

        pipeline.learnParameters(params["training"], params["test"], configMetadata)
        training = pipeline.predict(params["training"], configMetadata, False)
        test = pipeline.predict(params["test"], configMetadata, True)
        if "trainingDataRemediated" in pipelineParams and "testDataRemediated" in pipelineParams:
            params["allDataPreTransform"] = DataFrame.append(pipelineParams["trainingDataRemediated"], pipelineParams["testDataRemediated"])

        return (training, test, metadata)

    @overrides(Executor)
    def postProcessClassifier(self, clf, params):
        if clf != None:
            stateMachine = self.__setupJsonGenerationStateMachine()
            parser = params["parser"]
            mediator = stateMachine.getMediator()
            mediator.clf = clf
            mediator.modelLocalDir = params["modelLocalDir"]
            mediator.modelEnhancementsLocalDir = params["modelEnhancementsLocalDir"]
            mediator.pipelineLocalDir = params["pipelineLocalDir"]
            mediator.modelHdfsDir = params["modelHdfsDir"]
            mediator.allDataPreTransform = params["allDataPreTransform"]
            mediator.allDataPostTransform = params["allDataPostTransform"]
            mediator.data = params["test"]
            mediator.schema = params["schema"]
            mediator.pipeline = params["pipeline"]
            mediator.scoringPipeline = params["scoringPipeline"]
            mediator.depivoted = parser.isDepivoted()
            mediator.provenanceProperties = parser.getProvenanceProperties()
            mediator.algorithmProperties = parser.getAlgorithmProperties()
            mediator.metadata = params["metadata"]
            mediator.revenueColumn = parser.revenueColumn
            mediator.templateVersion = parser.templateVersion
            mediator.messages = []
            stateMachine.run()
        else:
            logger.warn("Generated classifier is null.")

    @overrides(Executor)
    def getModelDirPath(self, schema):
        return self.getModelDirByContainerId(schema)

    @overrides(Executor)
    def accept(self, filename):
        badSuffixes = [".p", ".dot", ".gz"]

        for badSuffix in badSuffixes:
            if filename.endswith(badSuffix):
                return False
        return True
