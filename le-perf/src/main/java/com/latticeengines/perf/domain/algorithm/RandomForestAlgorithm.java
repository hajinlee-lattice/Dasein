package com.latticeengines.perf.domain.algorithm;

public class RandomForestAlgorithm extends AlgorithmBase {

    public RandomForestAlgorithm() {
        setName("RF");
        setScript("/app/dataplatform/scripts/algorithm/rf_train.py");
        setAlgorithmProperties("criterion=gini n_estimators=10");
    }
}
