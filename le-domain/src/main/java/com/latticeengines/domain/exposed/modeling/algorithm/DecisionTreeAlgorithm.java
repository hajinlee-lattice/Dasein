package com.latticeengines.domain.exposed.modeling.algorithm;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

@Entity
@DiscriminatorValue("DecisionTree")
public class DecisionTreeAlgorithm extends AlgorithmBase {

    public DecisionTreeAlgorithm() {
        setName("DT");
        setScript("/datascience/dataplatform/scripts/algorithm/dt_train.py");
        setAlgorithmProperties("criterion=gini");
    }
}
