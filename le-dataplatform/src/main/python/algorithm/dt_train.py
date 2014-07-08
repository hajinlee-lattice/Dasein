from sklearn import tree

def train(trainingData, testData, schema, modelDir, algorithmProperties, runtimeProperties = None):
    X_train = trainingData[:, schema["featureIndex"]]
    Y_train = trainingData[:, schema["targetIndex"]]
    
    clf = tree.DecisionTreeClassifier(criterion = algorithmProperties["criterion"])
    
    clf.fit(X_train, Y_train)

    writeModel(schema, modelDir, clf)
    return clf

def writeModel(schema, modelDir, clf):
    classes = clf.classes_
    numClasses = len(classes)
    numInputs = len(clf.feature_importances_)
    
    fo = open(modelDir + "dt_model.txt", "w")
    fo.write("DecisionTreeClassifier\n")
    fo.write("LEDP Decision Tree Classifier Model\n")
    fo.write("classification\n")
    fo.write("binarySplit\n")
    fo.write(str(numInputs) + "\n")

    for i in range(0, numInputs):
        fo.write(schema["features"][i] + ",double,continuous,NA,NA,asMissing\n")

    fo.write(str(numClasses) + "\n")
    
    for i in range(0, numClasses):
        fo.write(str(classes[i]) + "\n")

    with open(modelDir + "tree.dot", 'w') as f:
        f = tree.export_graphviz(clf, out_file = f)
    
