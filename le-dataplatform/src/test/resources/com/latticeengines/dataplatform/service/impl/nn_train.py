def train(trainingData, testData, schema, modelDir, algorithmProperties):
    fo = open(modelDir + "model.txt", "w")
    fo.write("this is the generated model.")
    fo.close()
    print(trainingData)