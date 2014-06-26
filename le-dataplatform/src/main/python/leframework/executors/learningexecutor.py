import logging
import os
from leframework.codestyle import overrides
from leframework.executor import Executor
from leframework.model.statemachine import StateMachine
from leframework.model.states.averageprobabilitygenerator import AverageProbabilityGenerator
from leframework.model.states.bucketgenerator import BucketGenerator
from leframework.model.states.calibrationgenerator import CalibrationGenerator
from leframework.model.states.columnmetadatagenerator import ColumnMetadataGenerator
from leframework.model.states.finalize import Finalize
from leframework.model.states.initialize import Initialize
from leframework.model.states.modelgenerator import ModelGenerator
from leframework.model.states.summarygenerator import SummaryGenerator
from pipeline import EnumeratedColumnTransformStep
from pipeline import ImputationStep
from pipeline import Pipeline


logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='launcher')


class LearningExecutor(Executor):
    '''
    Base executor for all machine learning modeling. It does basic data preparation before
    passing the data into the modeling.
    '''


    def __init__(self):
        '''
        Constructor
        '''

    def __setupJsonGenerationStateMachine(self):
        stateMachine = StateMachine()
        stateMachine.addState(Initialize(), 1)  
        stateMachine.addState(CalibrationGenerator(), 4)
        stateMachine.addState(AverageProbabilityGenerator(), 2)
        stateMachine.addState(BucketGenerator(), 3)
        stateMachine.addState(ColumnMetadataGenerator(), 5)
        stateMachine.addState(ModelGenerator(), 6)
        stateMachine.addState(SummaryGenerator(), 7)
        stateMachine.addState(Finalize(), 8)
        return stateMachine

    @overrides(Executor)
    def transformData(self, params):
        training = params["training"]
        test = params["test"]
        stringColNames = params["parser"].getStringColumns()
        
        steps = [EnumeratedColumnTransformStep(stringColNames), ImputationStep()]
        pipeline = Pipeline(steps)
        params["pipeline"] = pipeline
        
        training = pipeline.predict(training).as_matrix()
        test = pipeline.predict(test).as_matrix()
        return (training, test)

    @overrides(Executor)
    def postProcessClassifier(self, clf, params):
        if clf != None:
            stateMachine = self.__setupJsonGenerationStateMachine()
            parser = params["parser"]
            mediator = stateMachine.getMediator()
            mediator.clf = clf
            mediator.modelLocalDir = params["modelLocalDir"]
            mediator.modelHdfsDir = params["modelHdfsDir"]
            mediator.data = params["test"]
            mediator.schema = params["schema"]
            mediator.target = mediator.data[:, mediator.schema["targetIndex"]]
            mediator.pipeline = params["pipeline"]
            mediator.depivoted = parser.isDepivoted()
            stateMachine.run()
        else:
            logger.error("Generated classifier is null!")
    
    @overrides(Executor)
    def getModelDirPath(self, schema):
        appIdList = os.environ['CONTAINER_ID'].split("_")[1:3]
        modelDirPath = "%s/%s" % (schema["model_data_dir"], "_".join(appIdList))
        return modelDirPath
    
    @overrides(Executor)
    def accept(self, filename):
        badSuffixes = [".p", ".dot", ".gz"]
        
        for badSuffix in badSuffixes:
            if filename.endswith(badSuffix):
                return False
        return True
        