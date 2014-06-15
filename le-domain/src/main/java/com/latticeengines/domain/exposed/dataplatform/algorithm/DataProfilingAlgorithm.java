package com.latticeengines.domain.exposed.dataplatform.algorithm;

public class DataProfilingAlgorithm extends AlgorithmBase {

    public DataProfilingAlgorithm() {
        setName("DP");
        setScript("/app/dataplatform/scripts/algorithm/feature_selection.py");
    }
}
