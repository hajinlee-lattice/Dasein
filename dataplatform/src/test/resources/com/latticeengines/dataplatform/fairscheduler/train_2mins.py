import os
import time

def train(trainingData, testData, schema, modelFile):
    print("Container %s" % (os.environ['CONTAINER_ID']))
    print("Sleeping for 2 mins...")
    time.sleep(120)
    print("Done")