package com.latticeengines.domain.exposed.dataplatform.algorithm;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "ALGORITHM")
@DiscriminatorValue("DataProfiling")
public class DataProfilingAlgorithm extends AlgorithmBase {

    public DataProfilingAlgorithm() {
        setName("DP");
        setScript("/app/dataplatform/scripts/algorithm/data_profile.py");
    }
}
