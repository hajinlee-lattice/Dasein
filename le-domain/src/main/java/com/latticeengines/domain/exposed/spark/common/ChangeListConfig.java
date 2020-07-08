package com.latticeengines.domain.exposed.spark.common;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class ChangeListConfig extends SparkJobConfig implements Serializable {

    public static final String NAME = "changeList";

    @JsonProperty("JoinKey")
    private String joinKey;

    @JsonProperty("ExclusionColumns")
    private List<String> exclusionColumns;

    // left=fromTable, right=toTable, default is outer join
    @JsonProperty("JoinType")
    private String joinType;

    @JsonProperty("CreationMode")
    private String creationMode = ChangeListConstants.CompleteMode;

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

    public List<String> getExclusionColumns() {
        return exclusionColumns;
    }

    public void setExclusionColumns(List<String> exclusionColumns) {
        this.exclusionColumns = exclusionColumns;
    }

    public String getCreationMode() {
        return creationMode;
    }

    public void setCreationMode(String creationMode) {
        this.creationMode = creationMode;
    }

}
