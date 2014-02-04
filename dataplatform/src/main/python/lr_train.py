import pandas as pd
import numpy as np
from sklearn.externals import joblib
from sklearn import cross_validation
from sklearn import linear_model
from sklearn import tree
from sklearn.qda import QDA
from sklearn.lda import LDA
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix
from sklearn.metrics import roc_curve, auc
import pylab as pl
import os

def train(trainingData, testData, schema, modelFile):
    X_train = trainingData[:, schema["featureIndex"]]
    Y_train = trainingData[:, schema["targetIndex"]]
    
    clf = tree.DecisionTreeClassifier()
    
    clf.fit(X_train, Y_train)
    
    modelFile = tree.export_graphviz(clf, out_file = modelFile)


