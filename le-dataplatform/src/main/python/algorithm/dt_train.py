from sklearn import tree

def train(trainingData, testData, schema, modelDir):
    X_train = trainingData[:, schema["featureIndex"]]
    Y_train = trainingData[:, schema["targetIndex"]]
    
    clf = tree.DecisionTreeClassifier()
    
    clf.fit(X_train, Y_train)

    writeModel(schema, modelDir, clf)

def writeModel(schema, modelDir, clf):
    with open(modelDir + "tree.dot", 'w') as f:
        f = tree.export_graphviz(clf, out_file = f)
    
