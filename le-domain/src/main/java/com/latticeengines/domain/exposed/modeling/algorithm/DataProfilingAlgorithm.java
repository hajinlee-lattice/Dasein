package com.latticeengines.domain.exposed.modeling.algorithm;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

@Entity
@DiscriminatorValue("DataProfiling")
public class DataProfilingAlgorithm extends AlgorithmBase {

    public DataProfilingAlgorithm() {
        setName("DP");
        setScript("/app/dataplatform/scripts/algorithm/data_profile.py");
        setPipelineScript("/app/dataplatform/scripts/pipeline.py");
        setPipelineLibScript("/app/dataplatform/scripts/lepipeline.tar.gz");
    }
}
