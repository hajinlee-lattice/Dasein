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
        proba = np.add(proba, models[i].predict_proba(X))
    return proba

class AggregatedModel(object):
    models = []
    n_models = 0

    def __init__(self):
        self.__build_model()
        print "Number of aggregated models:" + str(len(self.models))

    def __is_pickle_file(self, filename):
        return (filename.endswith('.p') or filename.endswith('.pkl') or filename.endswith('.pickle')) and ("STP" not in filename)

    def __build_model(self):
        self.models = []
        files = os.listdir(os.curdir)
        for f in files:
            if self.__is_pickle_file(f):
                with open(f, 'rb') as handle:
                    clf = pickle.load(handle)
                    self.models.append(clf)

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
        # Check data
        if getattr(X, "dtype", None) != DTYPE or X.ndim != 2:
            X = array2d(X, dtype=DTYPE)

        jobs, individual_models, starts = self.__partition_individual_models(self)

        all_proba = Parallel(n_jobs=jobs)(
            delayed(_parallel_predict_proba)(self.models[starts[i]:starts[i + 1]], X)
            for i in range(jobs))
        
        # Reduce
        proba = all_proba[0]
        for j in range(1, len(all_proba)):
            proba = np.add(proba, all_proba[j])
            
        proba = np.divide(proba, self.n_models)
        return proba