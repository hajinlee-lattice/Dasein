package com.latticeengines.domain.exposed.modeling.algorithm;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

@Entity
@DiscriminatorValue("PMML")
public class PMMLAlgorithm extends AlgorithmBase {

    public PMMLAlgorithm() {
        setName("PMML");
        setScript("/datascience/dataplatform/scripts/algorithm/pmml_model_skeleton.py");
        setAlgorithmProperties("");
        setPipelineDriver("/datascience/dataplatform/scripts/pmmlpipeline.json");
        setPipelineScript("/datascience/dataplatform/scripts/pipeline.py");
        setPipelineLibScript("/datascience/dataplatform/scripts/lepipeline.tar.gz");
    }

    @Override
    public boolean hasDataDiagnostics() {
        return false;
    }
}
