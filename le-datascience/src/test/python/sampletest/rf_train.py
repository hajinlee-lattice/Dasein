'''
  This module is the training driver script. This is what is specified in
  the analytic platform json driver file. In this sample, it is model.json 
  in the same directory for the key "python_script".
  
  The python_pipeline_script module needs to have one static method:
  
  train(trainingData, testData, schema, modelDir, algorithmProperties, runtimeProperties = None)
 
'''

from sklearn import ensemble
from sklearn import tree

'''
  This method needs to be in this script.
'''
def train(trainingData, testData, schema, modelDir, algorithmProperties, runtimeProperties = None):
    X_train = trainingData.as_matrix()[:, schema["featureIndex"]]
    Y_train = trainingData.as_matrix()[:, schema["targetIndex"]]
    
    clf = ensemble.RandomForestClassifier(criterion=algorithmProperties["criterion"],
                                          n_estimators=int(algorithmProperties["n_estimators"]),
                                          min_samples_split=int(algorithmProperties["min_samples_split"]),
                                          min_samples_leaf=int(algorithmProperties["min_samples_leaf"]),
                                          bootstrap=bool(algorithmProperties["bootstrap"]))
    
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
    
