package com.latticeengines.domain.exposed.spark.common;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class UpsertConfig extends SparkJobConfig {

    public static final String NAME = "upsert";

    // required parameters
    @JsonProperty("JoinKey")
    private String joinKey; // default join key for both sides

    // optional parameters
    @JsonProperty("SwitchSides")
    private Boolean switchSides; // when true, the second input is lhs

    @JsonProperty("ColsFromLhs")
    private List<String> colsFromLhs; // some special cols that needs to keep the value from lhs

    @JsonProperty("NotOverwriteByNull")
    private Boolean notOverwriteByNull; // if new data is null, it won't overwrite old data

    @JsonProperty("InputSystemBatch")
    private boolean addInputSystemBatch;

    @JsonProperty("BatchTemplateName")
    private String batchTemplateName;

    @JsonProperty("ExcludeAttrs")
    private List<String> excludeAttrs;

    @JsonProperty("EraseByNullEnabled")
    private boolean eraseByNullEnabled;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public static UpsertConfig joinBy(String joinKey) {
        UpsertConfig config = new UpsertConfig();
        config.setJoinKey(joinKey);
        return config;
    }

    public Boolean getSwitchSides() {
        return switchSides;
    }

    public void setSwitchSides(Boolean switchSides) {
        this.switchSides = switchSides;
    }

    public String getJoinKey() {
        return joinKey;
    }

    public void setJoinKey(String joinKey) {
        this.joinKey = joinKey;
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

    public boolean isAddInputSystemBatch() {
        return addInputSystemBatch;
    }

    public void setAddInputSystemBatch(boolean addInputSystemBatch) {
        this.addInputSystemBatch = addInputSystemBatch;
    }

    public String getBatchTemplateName() {
        return batchTemplateName;
    }

    public void setBatchTemplateName(String batchTemplateName) {
        this.batchTemplateName = batchTemplateName;
    }

    public List<String> getExcludeAttrs() {
        return excludeAttrs;
    }

    public void setExcludeAttrs(List<String> excludeAttrs) {
        this.excludeAttrs = excludeAttrs;
    }

    public boolean isEraseByNullEnabled() {
        return eraseByNullEnabled;
    }

    public void setEraseByNullEnabled(boolean eraseByNullEnabled) {
        this.eraseByNullEnabled = eraseByNullEnabled;
    }

}
