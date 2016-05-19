from leframework.executors.ruleexecutor import DataRuleExecutor
import logging

logging.basicConfig(level=logging.INFO, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='data_rule')

def getExecutor():
    return DataRuleExecutor()

def train(trainingData, testData, schema, modelDir, algorithmProperties, runtimeProperties=None, params=None):
    pass
