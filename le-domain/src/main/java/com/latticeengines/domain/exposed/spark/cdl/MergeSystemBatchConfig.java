package com.latticeengines.domain.exposed.spark.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class MergeSystemBatchConfig extends SparkJobConfig {

    public static final String NAME = "mergeSystems";

    // required parameters
    @JsonProperty("JoinKey")
    private String joinKey; // default join key for both sides

    @JsonProperty("NotOverwriteByNull")
    private Boolean notOverwriteByNull; // if new data is null, it won't overwrite old data

    @JsonProperty("KeepPrefix")
    private boolean keepPrefix; // if keep template as the prefix in the column
                                // name

    @JsonProperty("Templates")
    private List<String> templates; // increasing priority

    @JsonProperty("MinColumns")
    private List<String> minColumns; // increasing priority

    @JsonProperty("MaxColumns")
    private List<String> maxColumns; // increasing priority

    @JsonProperty("IdColumn")
    private String idColumn;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public static MergeSystemBatchConfig joinBy(String joinKey) {
        MergeSystemBatchConfig config = new MergeSystemBatchConfig();
        config.setJoinKey(joinKey);
        return config;
    }

    public String getJoinKey() {
        return joinKey;
    }

    public void setJoinKey(String joinKey) {
        this.joinKey = joinKey;
    }

    public Boolean getNotOverwriteByNull() {
        return notOverwriteByNull;
    }

    public void setNotOverwriteByNull(Boolean notOverwriteByNull) {
        this.notOverwriteByNull = notOverwriteByNull;
    }

    public List<String> getTemplates() {
        return templates;
    }

    public void setTemplates(List<String> templates) {
        this.templates = templates;
    }

    public boolean isKeepPrefix() {
        return keepPrefix;
    }

    public void setKeepPrefix(boolean keepPrefix) {
        this.keepPrefix = keepPrefix;
    }

    public List<String> getMinColumns() {
        return minColumns;
    }

    public void setMinColumns(List<String> minColumns) {
        this.minColumns = minColumns;
    }

    public List<String> getMaxColumns() {
        return maxColumns;
    }

    public void setMaxColumns(List<String> maxColumns) {
        this.maxColumns = maxColumns;
    }

    public String getIdColumn() { return idColumn; }

    public void setIdColumn(String idColumn) { this.idColumn = idColumn; }
}
