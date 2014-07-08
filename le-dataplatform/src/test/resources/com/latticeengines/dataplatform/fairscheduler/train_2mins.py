import os
import time

def train(trainingData, testData, schema, modelDir, algorithmProperties, runtimeProperties):
    print("Container %s" % (os.environ['CONTAINER_ID']))
    print("Sleeping for 2 min...")
    print("Start : %s" % time.ctime())
    time.sleep(120)
    print("End : %s" % time.ctime())
