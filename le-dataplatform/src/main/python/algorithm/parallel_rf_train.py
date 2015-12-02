from sklearn import ensemble
from sklearn import tree
from leframework.consolecapture import Capture
from leframework.consolecapture import CaptureMonitor
from leframework.executors.parallellearningexecutor import ParallelLearningExecutor
import math

def train(trainingData, testData, schema, modelDir, algorithmProperties, runtimeProperties=None, params=None):
    
    clf = trainClf(trainingData, testData, schema, modelDir, algorithmProperties, runtimeProperties, params)
    regressionClf = trainRegressionClf(trainingData, testData, schema, modelDir, algorithmProperties, runtimeProperties, params)
    
    return [clf] if regressionClf == None else [clf, regressionClf]
    

def trainClf (trainingData, testData, schema, modelDir, algorithmProperties, runtimeProperties=None, params=None):
    estimators = int(algorithmProperties.get("n_estimators", 100))
    clf = ensemble.RandomForestClassifier(criterion=algorithmProperties.get("criterion", "gini"),
                                          n_estimators=estimators,
                                          min_samples_split=int(algorithmProperties.get("min_samples_split", 25)),
                                          min_samples_leaf=int(algorithmProperties.get("min_samples_leaf", 10)),
                                          max_depth=int(algorithmProperties.get("max_depth", 8)),
                                          bootstrap=bool(algorithmProperties.get("bootstrap", True)),
                                          verbose=3)
    
    X_train = trainingData[schema["features"]]
    Y_train = trainingData[schema["target"]]
    with Capture() as capture:
        captureThread = CaptureMonitor(capture, 0.1, 0.12, estimators, runtimeProperties, "building tree.*");
        try:
            captureThread.start()
            clf.fit(X_train, Y_train)
        finally:
            captureThread.shutdown()

    writeModel(schema, modelDir, clf)
    return clf

def trainRegressionClf(trainingData, testData, schema, modelDir, algorithmProperties, runtimeProperties=None, params=None):
    
    parser = params["parser"]
    if parser.revenueColumn == None:
        return None
    
    regressionTraining = trainingData[trainingData[parser.revenueColumn] > 0]
    X_train = regressionTraining[schema["features"]]
    Y_train = regressionTraining[parser.revenueColumn]
    Y_train = Y_train.apply(lambda x : math.log(x + 1))
    
    estimators = int(algorithmProperties.get("n_estimators", 100))
    clf = ensemble.RandomForestRegressor( n_estimators=estimators,
                                          min_samples_split=int(algorithmProperties.get("min_samples_split", 25)),
                                          min_samples_leaf=int(algorithmProperties.get("min_samples_leaf", 10)),
                                          max_depth=int(algorithmProperties.get("max_depth", 8)),
                                          bootstrap=bool(algorithmProperties.get("bootstrap", True)),
                                          verbose=3)
    with Capture() as capture:
        captureThread = CaptureMonitor(capture, 0.22, 0.12, estimators, runtimeProperties, "building tree.*");
        try:
            captureThread.start()
            clf.fit(X_train, Y_train)
        finally:
            captureThread.shutdown()
            
    return clf

def writeModel(schema, modelDir, clf):
    estimators = clf.estimators_
    importances = clf.feature_importances_
    numInputs = len(importances)
    numTrees = len(estimators)

    fo = open(modelDir + "rf_model.txt", "w")
    fo.write("Column Name, Feature Importance\n")

    features = {}
    
    for i in range(0, numInputs):
        features[schema["features"][i]] = importances[i]
    features = sorted(features.items(), key = lambda x: x[1], reverse = True)
    
    for i in features:
        fo.write("%s, %f\n" % (i[0], i[1]))
        
    fo.close()

    for i in range(0, numTrees):
        filename = modelDir + "rf_" + str(i) + "_tree.dot"
        with open(filename, 'w') as f:
            f = tree.export_graphviz(estimators[i].tree_, out_file=f)

def getExecutor():
    return ParallelLearningExecutor()
