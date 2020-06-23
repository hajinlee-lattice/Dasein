package com.latticeengines.domain.exposed.spark.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class JoinChangeListAccountBatchConfig extends SparkJobConfig {
    public static final String NAME = "joinChangeListAccountBatch";

    @JsonProperty("JoinKey")
    private String joinKey;

    @JsonProperty("SelectColumns")
    private List<String> selectColumns;

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

    public List<String> getSelectColumns() {
        return selectColumns;
    }

    public void setSelectColumns(List<String> selectColumns) {
        this.selectColumns = selectColumns;
    }
}
