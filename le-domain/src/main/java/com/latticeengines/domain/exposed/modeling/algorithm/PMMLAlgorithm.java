package com.latticeengines.domain.exposed.modeling.algorithm;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

@Entity
@DiscriminatorValue("PMML")
public class PMMLAlgorithm extends AlgorithmBase {

    public PMMLAlgorithm() {
        setName("PMML");
        setScript("/app/dataplatform/scripts/algorithm/pmml_model_skeleton.py");
        setAlgorithmProperties("");
        setPipelineDriver("/app/dataplatform/scripts/pmmlpipeline.json");
        setPipelineScript("/app/dataplatform/scripts/pipeline.py");
        setPipelineLibScript("/app/dataplatform/scripts/lepipeline.tar.gz");
    }
}
