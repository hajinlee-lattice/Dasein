from leframework.aggregatedmodel import AggregatedModel
from leframework.executors.aggregationexecutor import AggregationExecutor

def train(trainingData, testData, schema, modelDir, algorithmProperties, runtimeProperties=None, params = None):
    # Creates the aggregated model which picks all pickle in current directory
    aggregatedclf =  AggregatedModel()
    return aggregatedclf

def getExecutor():
    return AggregationExecutor()
