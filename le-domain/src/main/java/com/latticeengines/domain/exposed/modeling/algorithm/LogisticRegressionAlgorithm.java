package com.latticeengines.domain.exposed.modeling.algorithm;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

@Entity
@DiscriminatorValue("LogisticRegression")
public class LogisticRegressionAlgorithm extends AlgorithmBase {

    public LogisticRegressionAlgorithm() {
        setName("LR");
        setScript("/datascience/dataplatform/scripts/algorithm/lr_train.py");
        setAlgorithmProperties("C=1.0");
    }
}
