def train(trainingData, testData, schema):
    print(trainingData)
    with open('/tmp/nn_trained.txt','w') as f:
        f.write("some model")
    return "/tmp/nn_trained.txt"