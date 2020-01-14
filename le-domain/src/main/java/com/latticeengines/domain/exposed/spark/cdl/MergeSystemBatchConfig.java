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
    private boolean keepPrefix; // if keep system as the prefix in the column name

    @JsonProperty("Systems")
    private List<String> systems; // increasing priority

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public static MergeSystemBatchConfig joinBy(String joinKey) {
        MergeSystemBatchConfig config =  new MergeSystemBatchConfig();
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

    public List<String> getSystems() {
        return systems;
    }

    public void setSystems(List<String> systems) {
        this.systems = systems;
    }

    public boolean isKeepPrefix() {
        return keepPrefix;
    }

    public void setKeepPrefix(boolean keepPrefix) {
        this.keepPrefix = keepPrefix;
    }

}
