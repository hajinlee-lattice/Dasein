package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class JoinConfig extends SparkJobConfig {

    public static final String NAME = "join";

    @JsonProperty("JoinKey")
    private String joinKey;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public String getJoinKey() {
        return joinKey;
    }

    public void setJoinKey(String joinKey) {
        this.joinKey = joinKey;
    }

}
