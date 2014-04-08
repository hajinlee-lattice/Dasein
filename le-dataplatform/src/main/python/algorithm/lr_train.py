from sklearn import linear_model

def train(trainingData, testData, schema, modelDir):
    X_train = trainingData[:, schema["featureIndex"]]
    Y_train = trainingData[:, schema["targetIndex"]]
    
    clf = linear_model.LogisticRegression()
    clf.fit(X_train, Y_train)
    
    writeModel(schema, modelDir, clf)

def writeModel(schema, modelDir, clf):
    coefficients = clf.coef_
    intercept = clf.intercept_

    numClasses = 1
    numInputs = len(coefficients[0])
    
    fo = open(modelDir + "model.txt", "w")
    fo.write("Logistic Regression Model\n")
    fo.write("classification\n")
    fo.write("logit\n")
    fo.write(str(numInputs) + "\n")
    
    for i in range(0, numInputs):
        fo.write(schema["features"][i] + ",double,continuous,NA,NA,asMissing\n")
        
    fo.write(str(numClasses) + "\n")
    
    for i in range(0, numClasses):
        fo.write(schema["targets"][i] + "\n")
    
    for i in range(0, numClasses):
        fo.write(str(intercept[i]) + "\n")
    
    for i in range(0, numClasses):
        for j in range(0, numInputs):
            fo.write(str(coefficients[i][j]) + "\n")
    
    fo.close()
