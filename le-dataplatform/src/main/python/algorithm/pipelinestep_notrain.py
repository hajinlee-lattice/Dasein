import logging
from leframework.executors.pipelinestepexecutor import PipelineStepExecutor

def train(trainingData, testData, schema, modelDir, algorithmProperties, runtimeProperties=None, params = None):
    logging.getLogger(name='dummy_train').info("NO TRAINING EXECUTED")
    return None

def getExecutor():
    return PipelineStepExecutor()
