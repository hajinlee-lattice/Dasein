package com.latticeengines.domain.exposed.dataplatform.algorithm;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "ALGORITHM")
@DiscriminatorValue("DecisionTree")
public class DecisionTreeAlgorithm extends AlgorithmBase {

    public DecisionTreeAlgorithm() {
        setName("DT");
        setScript("/app/dataplatform/scripts/algorithm/dt_train.py");
        setAlgorithmProperties("criterion=gini");
    }
}
