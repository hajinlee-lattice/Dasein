import logging

from pandas import DataFrame
from pandas import Series

from leframework.codestyle import overrides
from leframework.executor import Executor
from leframework.model.statemachine import StateMachine
from leframework.model.states.datacompositiongenerator import DataCompositionGenerator
from leframework.model.states.enhancedsummarygenerator import EnhancedSummaryGenerator
from leframework.model.states.initialize import Initialize
from leframework.model.states.modeldetailgenerator import ModelDetailGenerator
from leframework.model.states.namegenerator import NameGenerator
from leframework.model.states.pmmlcolumnmetadatagenerator import PmmlColumnMetadataGenerator
from leframework.model.states.pmmlcopyfile import PmmlCopyFile
from leframework.model.states.pmmlfinalize import PmmlFinalize
from leframework.model.states.pmmlmodelskeletongenerator import PmmlModelSkeletonGenerator
from leframework.model.states.provenancegenerator import ProvenanceGenerator


logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='pmmllearningexecutor')

class PmmlModelExecutor(Executor):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''

    def __setupJsonGenerationStateMachine(self):
        stateMachine = StateMachine()
        stateMachine.addState(Initialize(), 1)
        stateMachine.addState(PmmlColumnMetadataGenerator(), 2)
        stateMachine.addState(PmmlModelSkeletonGenerator(), 3)
        stateMachine.addState(PmmlCopyFile(), 4)
        stateMachine.addState(ModelDetailGenerator(), 5)
        stateMachine.addState(NameGenerator(), 6)
        stateMachine.addState(ProvenanceGenerator(), 7)
        stateMachine.addState(DataCompositionGenerator(), 8)
        stateMachine.addState(EnhancedSummaryGenerator(), 9)
        stateMachine.addState(PmmlFinalize(), 10)
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
    def preTransformData(self, training, test, params):
        schema = params["schema"]
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
        mediator.scoringPipeline = params["scoringPipeline"]
        mediator.depivoted = parser.isDepivoted()
        mediator.provenanceProperties = parser.getProvenanceProperties()
        mediator.algorithmProperties = parser.getAlgorithmProperties()
        mediator.metadata = params["metadata"]
        mediator.revenueColumn = parser.revenueColumn
        mediator.templateVersion = parser.templateVersion
        mediator.messages = []
        stateMachine.run()

    @overrides(Executor)
    def writeToHdfs(self, hdfs, params):
        super(PmmlModelExecutor, self).writeToHdfs(hdfs, params)

    @overrides(Executor)
    def getModelDirPath(self, schema):
        return self.getModelDirByContainerId(schema)

    @overrides(Executor)
    def accept(self, filename):
        return True