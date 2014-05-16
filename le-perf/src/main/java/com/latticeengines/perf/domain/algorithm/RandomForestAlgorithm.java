package com.latticeengines.perf.domain.algorithm;

public class RandomForestAlgorithm extends AlgorithmBase {

    public RandomForestAlgorithm() {
        setName("RF");
        setScript("/app/dataplatform/scripts/algorithm/rf_train.py");
        setAlgorithmProperties("criterion=gini n_estimators=200 n_jobs=4 min_samples_split=25 min_samples_leaf=10 bootstrap=True");
    }
}
