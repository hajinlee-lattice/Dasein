from sklearn import linear_model

def train(trainingData, testData, schema, modelFile):
    X_train = trainingData[:,0:4]
    Y_train = trainingData[:,4]
    
    clf = linear_model.LogisticRegression()
    clf.fit(X_train, Y_train)
    
    coefficients = clf.coef_
    intercept = clf.intercept_
    writeModel(modelFile, coefficients, intercept)

def writeModel(modelFile, coefficients, intercept):
    numCoeff = len(coefficients) 
    modelFile.write(str(numCoeff) + "\n")
    for i in range(0, numCoeff):
        modelFile.write(str(coefficients[i]) + "\n")
    
    modelFile.write(str(intercept) + "\n")
