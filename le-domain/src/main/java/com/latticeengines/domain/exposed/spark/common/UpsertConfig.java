package com.latticeengines.domain.exposed.spark.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class UpsertConfig extends SparkJobConfig {

    public static final String NAME = "upsert";

    // required parameters
    @JsonProperty("JoinKey")
    private String joinKey; // default join key for all rhs

    // optional parameters
    @JsonProperty("LhsIdx")
    private Integer lhsIdx; // which input is lhs, default is 0

    @JsonProperty("RhsSeq")
    private List<Integer> rhsSeq; // order of rhs, default is 1,2,...

    @JsonProperty("ColsFromLhs")
    private List<String> colsFromLhs; // some special cols that needs to keep the value from lhs

    @JsonProperty("NotOverwriteByNull")
    private Boolean notOverwriteByNull; // if new data is null, it won't overwrite old data

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public static UpsertConfig joinBy(String joinKey) {
        return joinBy(joinKey, null);
    }

    public static UpsertConfig joinBy(String joinKey, Collection<String> colsFromLhs) {
        UpsertConfig config =  new UpsertConfig();
        config.setJoinKey(joinKey);
        if (CollectionUtils.isNotEmpty(colsFromLhs)) {
            config.setColsFromLhs(new ArrayList<>(colsFromLhs));
        }
        return config;
    }

    public String getJoinKey() {
        return joinKey;
    }

    public void setJoinKey(String joinKey) {
        this.joinKey = joinKey;
    }

    public Integer getLhsIdx() {
        return lhsIdx;
    }

    public void setLhsIdx(Integer lhsIdx) {
        this.lhsIdx = lhsIdx;
    }

    public List<Integer> getRhsSeq() {
        return rhsSeq;
    }

    public void setRhsSeq(List<Integer> rhsSeq) {
        this.rhsSeq = rhsSeq;
    }

    public List<String> getColsFromLhs() {
        return colsFromLhs;
    }

    public void setColsFromLhs(List<String> colsFromLhs) {
        this.colsFromLhs = colsFromLhs;
    }

    public Boolean getNotOverwriteByNull() {
        return notOverwriteByNull;
    }

    public void setNotOverwriteByNull(Boolean notOverwriteByNull) {
        this.notOverwriteByNull = notOverwriteByNull;
    }
}
