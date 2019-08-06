package com.latticeengines.domain.exposed.spark;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TestPartitionJobConfig extends SparkJobConfig{

    public static final String NAME = "testPartition";

    @JsonProperty("Partition")
    private Boolean partition;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public Boolean getPartition() {
        return partition;
    }

    public void setPartition(Boolean partition) {
        this.partition = partition;
    }

}
