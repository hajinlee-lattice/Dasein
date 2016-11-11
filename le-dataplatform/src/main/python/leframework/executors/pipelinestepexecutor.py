import logging
#import os
#import shutil
#from pandas import Series
from pandas import DataFrame

from leframework.codestyle import overrides
from leframework.executor import Executor


logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='pipelinestepexecutor')


class PipelineStepExecutor(Executor):

    def __init__(self):
        pass

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
        pass

    @overrides(Executor)
    def preTransformData(self, training, test, params):
        params["allDataPreTransform"] = DataFrame.append(training, test)
        return (training, test)

    @overrides(Executor)
    def postTransformData(self, training, test, params):
        params["allDataPostTransform"] = DataFrame.append(training, test)
        return (training, test)


    @overrides(Executor)
    def transformData(self, params):
        logger.info('CONFIGURING PIPELINE STEP')
        pipeline, metadata, pipelineParams = self.createDataPipeline(params)
        configMetadata = params["schema"]["config_metadata"]["Metadata"] if params["schema"]["config_metadata"] is not None else None
        logger.info('PIPELINE STEP CONFIGURED')
        
        step = pipeline.getPipeline()[0]
        step.setMediator(pipeline.getMediator())

        logger.info('RUNNING: learnParameters')
        step.learnParameters(params["training"], params["test"], configMetadata)

        logger.info('RUNNING: transform')
        training = params["training"] = step.transform(params["training"], configMetadata, False)
        test = params["test"] = step.transform(params["test"], configMetadata, True)

        logger.info('VALIDATION: getInputColumns()\n{}'.format(str(step.getInputColumns())))
        logger.info('VALIDATION: getOutputColumns()\n{}'.format(str(step.getOutputColumns())))
        logger.info('VALIDATION: getRTSMainModule()\n{}'.format(str(step.getRTSMainModule())))
        logger.info('VALIDATION: getRTSArtifacts()\n{}'.format(str(step.getRTSArtifacts())))

        return (training, test, metadata)


    @overrides(Executor)
    def getModelDirPath(self, schema):
        return '/'

    @overrides(Executor)
    def accept(self, filename):
        pass

    @overrides(Executor)
    def postProcessClassifier(self, clf, params):
        pass

