package com.latticeengines.domain.exposed.dataplatform.algorithm;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "ALGORITHM")
@DiscriminatorValue("LogisticRegression")
public class LogisticRegressionAlgorithm extends AlgorithmBase {

    public LogisticRegressionAlgorithm() {
        setName("LR");
        setScript("/app/dataplatform/scripts/algorithm/lr_train.py");
        setAlgorithmProperties("C=1.0");
    }
}
