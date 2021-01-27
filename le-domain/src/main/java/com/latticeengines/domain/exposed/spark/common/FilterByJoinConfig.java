package com.latticeengines.domain.exposed.spark.common;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class FilterByJoinConfig extends SparkJobConfig {
    public static final String NAME = "filterByJoin";

    @JsonProperty("Key")
    private String key;

    @JsonProperty("JoinType")
    private String joinType; // Support join types are: "inner", "outer", "full", "full_outer", "left",
                             // "left_outer", "right", "right_outer", "left_semi", "left_anti"

    @JsonProperty("SelectColumns")
    private List<String> selectColumns;

    @JsonProperty("SwitchSide")
    private Boolean switchSide; // when true, reverse the order of the input

    @Override
    public String getName() {
        return NAME;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getJoinType() {
        return joinType;
    }

    public void setJoinType(String joinType) {
        this.joinType = joinType;
    }

    public List<String> getSelectColumns() {
        return selectColumns;
    }

    public void setSelectColumns(List<String> selectColumns) {
        this.selectColumns = selectColumns;
    }

    public Boolean getSwitchSide() {
        return switchSide;
    }

    public void setSwitchSide(Boolean switchSide) {
        this.switchSide = switchSide;
    }
}
