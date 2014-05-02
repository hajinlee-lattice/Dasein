package com.latticeengines.perf.domain.algorithm;

public class DecisionTreeAlgorithm extends AlgorithmBase {

    public DecisionTreeAlgorithm() {
        setName("DT");
        setScript("/app/dataplatform/scripts/algorithm/dt_train.py");
        setAlgorithmProperties("criterion=gini");
    }
}
