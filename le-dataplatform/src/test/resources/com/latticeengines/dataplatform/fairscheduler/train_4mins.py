import os
import time

def train(trainingData, testData, schema, modelFile):
    print("Container %s" % (os.environ['CONTAINER_ID']))
    print("Sleeping for 4 min...")
    print("Start : %s" % time.ctime())
    time.sleep(240)
    print("End : %s" % time.ctime())
