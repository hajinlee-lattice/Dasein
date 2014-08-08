import logging
import os

import encoder
import fastavro as avro
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
from leframework.model.states.namegenerator import NameGenerator
from leframework.model.states.percentilebucketgenerator import PercentileBucketGenerator
from leframework.model.states.summarygenerator import SummaryGenerator
from pipeline import EnumeratedColumnTransformStep
from pipeline import ImputationStep
from pipeline import Pipeline


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
        stateMachine.addState(CalibrationGenerator(), 4)
        stateMachine.addState(AverageProbabilityGenerator(), 2)
        stateMachine.addState(BucketGenerator(), 3)
        stateMachine.addState(ColumnMetadataGenerator(), 5)
        stateMachine.addState(ModelGenerator(), 6)
        stateMachine.addState(SummaryGenerator(), 7)
        stateMachine.addState(NameGenerator(), 8)
        stateMachine.addState(PercentileBucketGenerator(), 9)
        stateMachine.addState(Finalize(), 10)
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
                        record["hashValue"] = encoder.transform(record["columnvalue"])
                
                    if colname in metadata:
                        metadata[colname].append(record)
                    else:
                        metadata[colname] = [record]
                
                    realColNameToRecord[sqlcolname] = [record]
        return (metadata, realColNameToRecord)
    
    def getDecoratedColumns(self, metadata):
        stringColumns = dict()
        continuousColumns = dict()
        transform = encoder.HashEncoder()
        
        for key, value in metadata.iteritems():
            if value[0]["Dtype"] == "STR":
                stringColumns[key] = transform
            else:
                continuousColumns[key] = value[0]["median"]
        
        return (stringColumns, continuousColumns)

    @overrides(Executor)
    def transformData(self, params):
        metadata = self.retrieveMetadata(params["schema"]["data_profile"], params["parser"].isDepivoted())
        (stringColumns, continuousColumns) = self.getDecoratedColumns(metadata[0])
        training = params["training"]
        test = params["test"]
        
        steps = [EnumeratedColumnTransformStep(stringColumns), ImputationStep(continuousColumns)]
        pipeline = Pipeline(steps)
        params["pipeline"] = pipeline
        
        training = pipeline.predict(training)
        test = pipeline.predict(test)
        return (training, test, metadata)

    @overrides(Executor)
    def postProcessClassifier(self, clf, params):
        if clf != None:
            stateMachine = self.__setupJsonGenerationStateMachine()
            parser = params["parser"]
            mediator = stateMachine.getMediator()
            mediator.clf = clf
            mediator.modelLocalDir = params["modelLocalDir"]
            mediator.modelHdfsDir = params["modelHdfsDir"]
            mediator.data = params["test"].as_matrix()
            mediator.schema = params["schema"]
            mediator.target = mediator.data[:, mediator.schema["targetIndex"]]
            mediator.pipeline = params["pipeline"]
            mediator.depivoted = parser.isDepivoted()
            mediator.provenanceProperties = parser.getProvenanceProperties()
            mediator.metadata = params["metadata"]
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
