package com.latticeengines.domain.exposed.modeling.algorithm;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

@Entity
@DiscriminatorValue("Aggregation")
public class AggregationAlgorithm extends AlgorithmBase {

    public AggregationAlgorithm() {
        setName("AG");
        setScript("/app/dataplatform/scripts/algorithm/aggregate_train.py");
    }
}
