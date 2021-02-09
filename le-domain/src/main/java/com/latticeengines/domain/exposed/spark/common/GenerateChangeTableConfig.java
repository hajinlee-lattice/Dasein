package com.latticeengines.domain.exposed.spark.common;

import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class GenerateChangeTableConfig extends SparkJobConfig {
    public static final String NAME = "GenerateChangeTable";

    @JsonProperty("JoinKey")
    private String joinKey;

    @JsonProperty("ExclusionColumns")
    private List<String> exclusionColumns;

    // retain previous value
    @JsonProperty("attrsForbidToSet")
    private Set<String> attrsForbidToSet;

    @Override
    public String getName() {
        return NAME;
    }

    public String getJoinKey() {
        return joinKey;
    }

    public void setJoinKey(String joinKey) {
        this.joinKey = joinKey;
    }

    public List<String> getExclusionColumns() {
        return exclusionColumns;
    }

    public void setExclusionColumns(List<String> exclusionColumns) {
        this.exclusionColumns = exclusionColumns;
    }

    public Set<String> getAttrsForbidToSet() {
        return attrsForbidToSet;
    }

    public void setAttrsForbidToSet(Set<String> attrsForbidToSet) {
        this.attrsForbidToSet = attrsForbidToSet;
    }
}
