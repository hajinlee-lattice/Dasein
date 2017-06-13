from sklearn import ensemble
from sklearn import tree
import pandas as pd
from leframework.consolecapture import Capture
from leframework.consolecapture import CaptureMonitor
from pipelinefwk import get_logger

logger = get_logger("algorithm")

def train(trainingData, testData, schema, modelDir, algorithmProperties, runtimeProperties=None, params = None):
    logger.info("Record count:" + str(trainingData.shape[0]))
    if '__TRAINING__' in trainingData.columns.values:
        trainingData = trainingData[trainingData['__TRAINING__'] == 1]
        logger.info("Record count after selection:" + str(trainingData.shape[0]))
        
    X_train = trainingData[schema["features"]]
    Y_train = trainingData[schema["target"]]

    if X_train.isnull().any().any():
        pd.set_option('display.max_rows', 1000)
        logger.error('Features having None/NaN')
        logger.error('\n{}'.format(X_train.isnull().any()))
        pd.reset_option('display.max_rows')

    estimators = int(algorithmProperties.get("n_estimators", 100))
    randomState = algorithmProperties.get("random_state")

    if randomState is not None:
        randomState = int(randomState)

    clf = ensemble.RandomForestClassifier(criterion=algorithmProperties.get("criterion", "gini"),
                                          n_estimators=estimators,
                                          min_samples_split=int(algorithmProperties.get("min_samples_split", 25)),
                                          min_samples_leaf=int(algorithmProperties.get("min_samples_leaf", 10)),
                                          max_depth=int(algorithmProperties.get("max_depth", 8)),
                                          bootstrap=bool(algorithmProperties.get("bootstrap", True)),
                                          random_state=randomState,
                                          verbose=3)
    with Capture() as capture:
        captureThread = CaptureMonitor(capture, 0.1, 0.233, estimators, runtimeProperties, "building tree.*");
        try:
            captureThread.start()
            clf.fit(X_train, Y_train)
        finally:
            captureThread.shutdown()

    writeModel(schema, modelDir, clf)
    
    exportTrainingData(trainingData)
    
    return clf

def exportTrainingData(trainingData):
    columns = list(trainingData.columns.values)
    columns = [x for x in columns if not x.startswith("###") and not x.startswith("__")]
    trainingData.to_csv("exportrftrain.csv", sep=',', encoding='utf-8', cols=columns, index=False, float_format='%.10f')
    
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

