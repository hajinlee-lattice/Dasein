from sklearn import linear_model

def train(trainingData, testData, schema, modelDir, algorithmProperties, runtimeProperties = None):
    X_train = trainingData.as_matrix()[:, schema["featureIndex"]]
    Y_train = trainingData.as_matrix()[:, schema["targetIndex"]]
    
    clf = linear_model.LogisticRegression(C = float(algorithmProperties["C"]))
    clf.fit(X_train, Y_train)
    
    writeModel(schema, modelDir, clf)
    return clf

def writeModel(schema, modelDir, clf):
    coefficients = clf.coef_
    intercept = clf.intercept_

    classes = clf.classes_
    numClasses = len(classes)
    numInputs = len(coefficients[0])
    
    fo = open(modelDir + "lr_model.txt", "w")
    fo.write("LogisticRegression\n")
    fo.write("LEDP Logistic Regression Model\n")
    fo.write("classification\n")
    fo.write("logit\n")
    fo.write(str(numInputs) + "\n")
    
    for i in range(0, numInputs):
        fo.write(schema["features"][i] + ",double,continuous,NA,NA,asMissing\n")

    fo.write(str(numClasses) + "\n")
    
    for i in range(0, numClasses):
        fo.write(str(classes[i]) + "\n")

    for i in range(0, len(intercept)):
        fo.write(str(intercept[i]) + "\n")

    for i in range(0, len(coefficients)):
        for j in range(0, numInputs):
            fo.write(str(coefficients[i][j]) + "\n")

    fo.close()
