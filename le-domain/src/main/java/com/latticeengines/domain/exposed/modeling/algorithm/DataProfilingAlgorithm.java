package com.latticeengines.domain.exposed.modeling.algorithm;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

@Entity
@DiscriminatorValue("DataProfiling")
public class DataProfilingAlgorithm extends AlgorithmBase {

    public DataProfilingAlgorithm() {
        setName("DP");
        setScript("/datascience/dataplatform/scripts/algorithm/data_profile.py");
        setPipelineScript("/datascience/dataplatform/scripts/pipeline.py");
        setPipelineLibScript("/datascience/dataplatform/scripts/lepipeline.tar.gz");
    }
}
