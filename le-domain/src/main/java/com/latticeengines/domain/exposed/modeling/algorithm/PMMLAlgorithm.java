package com.latticeengines.domain.exposed.modeling.algorithm;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

@Entity
@DiscriminatorValue("PMML")
public class PMMLAlgorithm extends AlgorithmBase {

    public PMMLAlgorithm() {
        setName("PMML");
        setScript("/app/dataplatform/scripts/algorithm/do_nothing.py");
        setAlgorithmProperties("");
        setPipelineScript("configurablepipeline.py");
    }
}
