def train(trainingData, testData, schema):
    print(trainingData)
    with open('/tmp/lr_trained.txt','w') as f:
        f.write("some model")
    return "/tmp/lr_trained.txt"