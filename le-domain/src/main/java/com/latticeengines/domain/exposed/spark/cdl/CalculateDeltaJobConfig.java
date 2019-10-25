package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class CalculateDeltaJobConfig extends SparkJobConfig {
    public static final String NAME = "calculateDelta";

    @JsonProperty("OldData")
    private DataUnit oldData;
    @JsonProperty("NewData")
    private DataUnit newData;
    @JsonProperty("FilterJoinKeyNulls")
    private Boolean filterJoinKeyNulls = false;
    @JsonProperty("JoinKey")
    private String joinKey;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public CalculateDeltaJobConfig() {
    }

    public CalculateDeltaJobConfig(DataUnit newData, DataUnit oldData, String joinKey, boolean filterJoinKeyNulls) {
        this.newData = newData;
        this.oldData = oldData;
        this.joinKey = joinKey;
        this.filterJoinKeyNulls = filterJoinKeyNulls;
    }

    @Override
    public int getNumTargets() {
        return 2;
    }

    public DataUnit getOldData() {
        return oldData;
    }

    public void setOldData(DataUnit oldData) {
        this.oldData = oldData;
    }

    public DataUnit getNewData() {
        return newData;
    }

    public void setNewData(DataUnit newData) {
        this.newData = newData;
    }

    public Boolean isFilterJoinKeyNulls() {
        return filterJoinKeyNulls;
    }

    public void setFilterJoinKeyNulls(Boolean filterJoinKeyNulls) {
        this.filterJoinKeyNulls = filterJoinKeyNulls;
    }

    public String getJoinKey() {
        return joinKey;
    }

    public void setJoinKey(String joinKey) {
        this.joinKey = joinKey;
    }
}
