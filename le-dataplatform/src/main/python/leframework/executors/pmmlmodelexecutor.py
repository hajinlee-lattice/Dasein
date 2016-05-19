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
from leframework.model.states.pmmlfinalize import PmmlFinalize
from leframework.model.states.pmmlmodelskeletongenerator import PmmlModelSkeletonGenerator
from leframework.model.states.pmmlcopyfile import PmmlCopyFile


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
        stateMachine.addState(DataCompositionGenerator(), 7)
        stateMachine.addState(EnhancedSummaryGenerator(), 8)
        stateMachine.addState(PmmlFinalize(), 9)
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
        metadata = self.retrieveMetadata(params["schema"]["data_profile"], params["parser"].isDepivoted())
        configMetadata = params["schema"]["config_metadata"]["Metadata"] if params["schema"]["config_metadata"] is not None else None 
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
        
        # Create the data pipeline
        pipeline, scoringPipeline = globals()["setupPipeline"](pipelineDriver, \
                                                               pipelineLib, \
                                                               metadata[0], \
                                                               stringColumns, \
                                                               params["parser"].target, \
                                                               pipelineProps)
        params["pipeline"] = pipeline
        params["scoringPipeline"] = scoringPipeline
        pipeline.learnParameters(params["training"], params["test"], configMetadata)
        training = pipeline.predict(params["training"], configMetadata, False)
        test = pipeline.predict(params["test"], configMetadata, True)
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
        return schema["model_data_dir"]
    
    @overrides(Executor)
    def accept(self, filename):
        return True