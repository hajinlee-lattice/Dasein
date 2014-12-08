import os
import time

def train(trainingData, testData, schema, modelDir, algorithmProperties, runtimeProperties, params):
    print("Container %s" % (os.environ['CONTAINER_ID']))
    print("Sleeping for 5 min...")
    print("Start : %s" % time.ctime())
    time.sleep(300)
    print("End : %s" % time.ctime())
