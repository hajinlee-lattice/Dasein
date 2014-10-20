from sklearn import ensemble
from sklearn import tree

def train(trainingData, testData, schema, modelDir, algorithmProperties, runtimeProperties = None):
    X_train = trainingData.as_matrix()[:, schema["featureIndex"]]
    Y_train = trainingData.as_matrix()[:, schema["targetIndex"]]

    clf = ensemble.RandomForestClassifier(criterion=algorithmProperties.get("criterion", "gini"),
                                          n_estimators=int(algorithmProperties.get("n_estimators", 100)),
                                          min_samples_split=int(algorithmProperties.get("min_samples_split", 25)),
                                          min_samples_leaf=int(algorithmProperties.get("min_samples_leaf", 10)),
                                          max_depth=int(algorithmProperties.get("max_depth", 8)),
                                          bootstrap=bool(algorithmProperties.get("bootstrap", True)))

    clf.fit(X_train, Y_train)

    writeModel(schema, modelDir, clf)
    return clf

def writeModel(schema, modelDir, clf):
    estimators = clf.estimators_
    importances = clf.feature_importances_
    classes = clf.classes_

    numClasses = len(classes)
    numInputs = len(importances)
    numTrees = len(estimators)

    fo = open(modelDir + "rf_model.txt", "w")
    fo.write("RandomForestClassifier\n")
    fo.write("LEDP Random Forest Classifier Model\n")
    fo.write("classification\n")
    fo.write("binarySplit\n")
    fo.write(str(numInputs) + "\n")

    for i in range(0, numInputs):
        fo.write(schema["features"][i] + ",double,continuous,NA,NA,asMissing\n")

    fo.write(str(numClasses) + "\n")

    for i in range(0, numClasses):
        fo.write(str(classes[i]) + "\n")

    fo.write(str(numTrees) + "\n")

    fo.close()

    for i in range(0, numTrees):
        filename = modelDir + "rf_" + str(i) + "_tree.dot"
        with open(filename, 'w') as f:
            f = tree.export_graphviz(estimators[i].tree_, out_file = f)

