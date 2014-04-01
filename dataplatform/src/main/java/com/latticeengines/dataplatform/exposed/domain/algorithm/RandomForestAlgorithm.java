package com.latticeengines.dataplatform.exposed.domain.algorithm;

public class RandomForestAlgorithm extends AlgorithmBase {

    public RandomForestAlgorithm() {
        setName("RF");
        setScript("/app/dataplatform/scripts/algorithm/rf_train.py");
    }
}
