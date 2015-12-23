"""Aggregated model

This model is used by the aggregationExecutor called by the Reducer.

The model retrieves from local directory all N pickled models (files with extension .p, .pkl, or .pickle),
each was created (and pickled) by a Mapper.

It can perform predictions based on majority/average predictions among the N models in
classification/scoring (regression) tasks.

The module structure is the following:

- The AggregatedModel class which contains two methods. The ``build_model``
  retrieves pickled models. The ``predict_proba`` method returns the average of N
  probabilities (scores) predicted by the models.

"""

from __future__ import division

import os
import numpy as np
import pickle

from sklearn.utils import array2d
from sklearn.externals.joblib import Parallel, delayed, cpu_count
from sklearn.tree._tree import DTYPE

__all__ = ["AggregatedModelClassifier"]
MAX_INT = np.iinfo(np.int32).max

def _parallel_predict_proba(models, X):
    proba = models[0].predict_proba(X)
    for i in (range(1, len(models))):
        preds = models[i].predict_proba(X)
        if proba.shape == preds.shape:
            proba = np.add(proba, preds)
        else:
            minRows = min(len(proba), len(preds))
            minCols = min(len(proba[0]), len(preds[0]))
            proba = np.add(proba[0:minRows, 0:minCols], preds[0:minRows, 0:minCols])

    return proba

def _parallel_predict_regression(models, X):
    if models is None or len(models) == 0:
        return np.zeros(X.shape[0])
    regression = models[0].predict(X)
    for i in (range(1, len(models))):
        preds = models[i].predict(X)
        if regression.shape == preds.shape:
            regression = np.add(regression, preds)
        else:
            minRows = min(len(regression), len(preds))
            minCols = min(len(regression[0]), len(preds[0]))
            regression = np.add(regression[0:minRows, 0:minCols], preds[0:minRows, 0:minCols])

    return regression

class AggregatedModel(object):
    regressionModels = []
    models = []
    n_models = 0
    
    def __init__(self):
        self.__build_model()
        print "Number of aggregated models:" + str(len(self.models))
        print "Number of aggregated regression models:" + str(len(self.regressionModels))

    def __is_pickle_file(self, filename):
        return (filename.endswith('.p') or filename.endswith('.pkl') or filename.endswith('.pickle')) and ("STP" not in filename)

    def __build_model(self):
        self.models = []
        self.regressionModels = []
        files = os.listdir(os.curdir)
        for f in files:
            if self.__is_pickle_file(f):
                with open(f, 'rb') as handle:
                    clfs = pickle.load(handle)
                    self.models.append(clfs[0])
                    if len(clfs) > 1:
                        self.regressionModels.append(clfs[1])

        self.n_models = len(self.models)

    def __partition_individual_models(self, aggrregatedmodel):
        """Private function used to partition individual models between jobs. To be used to paralellize the aggregated model."""
        # Compute the number of jobs
        n_jobs = min(cpu_count(), aggrregatedmodel.n_models)

        # Individual models between jobs
        n_individual_models = [aggrregatedmodel.n_models // n_jobs] * n_jobs

        for i in range(aggrregatedmodel.n_models  % n_jobs):
            n_individual_models[i] += 1

        starts = [0] * (n_jobs + 1)

        for i in range(1, n_jobs + 1):
            starts[i] = starts[i - 1] + n_individual_models[i - 1]

        return n_jobs, n_individual_models, starts

    def predict_proba(self, X):
        """Predict aggregate class probabilities for X.

        The predicted aggregate class probabilities of an input sample is computed as
        the mean predicted class probabilities of the multiple models.

        Parameters
        ----------
        X : array-like of shape = [n_samples, n_features]
            The input samples.

        Returns
        -------
        proba : array of shape = [n_samples, n_classes], or a list of n_outputs
                such arrays if n_outputs > 1.
                The class probabilities of the input samples. Classes are
                ordered by arithmetical order.
        """
        return self.predict(X, True)
    
    def predict_regression(self, X):
        if (len(self.regressionModels) == 0):
            return None
        return self.predict(X, False)
    
    def predict(self, X, isPredictPossibility):
        
        # Check data
        if getattr(X, "dtype", None) != DTYPE or X.ndim != 2:
            X = array2d(X, dtype=DTYPE)

        jobs, individual_models, starts = self.__partition_individual_models(self)

        if isPredictPossibility:
            all_values = Parallel(n_jobs=1)(
                delayed(_parallel_predict_proba)(self.models[starts[i]:starts[i + 1]], X)
                for i in range(jobs))
        else:
            all_values = Parallel(n_jobs=1)(
                delayed(_parallel_predict_regression)(self.regressionModels[starts[i]:starts[i + 1]], X)
                for i in range(jobs))
                
        # Reduce
        value = all_values[0]
        for j in range(1, len(all_values)):
            if value.shape == all_values[j].shape:
                value = np.add(value, all_values[j])
            else:
                minRows = min(len(value), len(all_values[j]))
                minCols = min(len(value[0]), len(all_values[j][0]))
                value = np.add(value[0:minRows, 0:minCols], all_values[j][0:minRows, 0:minCols])
                
        value = np.divide(value, self.n_models)
        return value
    
