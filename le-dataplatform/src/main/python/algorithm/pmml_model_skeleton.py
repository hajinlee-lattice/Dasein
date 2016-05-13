from leframework.executors.pmmlmodelexecutor import PmmlModelExecutor
import logging

logging.basicConfig(level=logging.INFO, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='pmml_model_skeleton')

def getExecutor():
    return PmmlModelExecutor()

def train(trainingData, testData, schema, modelDir, algorithmProperties, runtimeProperties=None, params=None):
    pass
