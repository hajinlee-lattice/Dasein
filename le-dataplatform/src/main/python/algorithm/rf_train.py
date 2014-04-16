from sklearn import ensemble
from sklearn import tree

def train(trainingData, testData, schema, modelDir, algorithmProperties):
    X_train = trainingData[:, schema["featureIndex"]]
    Y_train = trainingData[:, schema["targetIndex"]]
    
    clf = ensemble.RandomForestClassifier()
    
    clf.fit(X_train, Y_train)

    writeModel(schema, modelDir, clf)

def writeModel(schema, modelDir, clf):
    estimators = clf.estimators_
    importances = clf.feature_importances_
    
    numClasses = clf.n_classes
    
    with open(modelDir + "tree.dot", 'w') as f:
        f = tree.export_graphviz(clf, out_file = f)
    
