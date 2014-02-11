import os
import time

def train(trainingData, testData, schema, modelFile):
    print("Container %s" % (os.environ['CONTAINER_ID']))
    print("Sleeping for 1 min...")
    time.sleep(60)
    print("Done")