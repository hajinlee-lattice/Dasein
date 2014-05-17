package com.latticeengines.domain.exposed.dataplatform.algorithm;

public class LogisticRegressionAlgorithm extends AlgorithmBase {

    public LogisticRegressionAlgorithm() {
        setName("LR");
        setScript("/app/dataplatform/scripts/algorithm/lr_train.py");
        setAlgorithmProperties("C=1.0");
    }
}
