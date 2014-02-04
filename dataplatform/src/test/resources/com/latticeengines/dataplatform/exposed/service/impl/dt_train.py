from sklearn import tree

def train(trainingData, testData, schema, modelFile):
    X_train = trainingData[:, schema["featureIndex"]]
    Y_train = trainingData[:, schema["targetIndex"]]
    
    clf = tree.DecisionTreeClassifier()
    
    clf.fit(X_train, Y_train)
    
    modelFile = tree.export_graphviz(clf, out_file = modelFile)
