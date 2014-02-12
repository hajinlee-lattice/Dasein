import os
import time

def train(trainingData, testData, schema, modelFile):
    print("Container %s" % (os.environ['CONTAINER_ID']))
    print("Sleeping for 5 min...")
    print("Start : %s" % time.ctime())
    time.sleep(300)
    print("End : %s" % time.ctime())
