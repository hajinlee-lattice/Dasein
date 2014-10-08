package com.latticeengines.domain.exposed.modeling.algorithm;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "ALGORITHM")
@DiscriminatorValue("RandomForest")
public class RandomForestAlgorithm extends AlgorithmBase {

    public RandomForestAlgorithm() {
        setName("RF");
        setScript("/app/dataplatform/scripts/algorithm/rf_train.py");
        setAlgorithmProperties("criterion=gini n_estimators=200 n_jobs=4 min_samples_split=25 min_samples_leaf=10 bootstrap=True");
    }
}
