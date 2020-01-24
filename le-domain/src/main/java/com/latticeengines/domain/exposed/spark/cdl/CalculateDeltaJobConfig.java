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
    @JsonProperty("FilterPrimaryJoinKeyNulls")
    private Boolean filterPrimaryJoinKeyNulls = false;
    @JsonProperty("PrimaryJoinKey")
    private String primaryJoinKey;
    @JsonProperty("SecondaryJoinKey")
    private String secondaryJoinKey;

    public CalculateDeltaJobConfig() {
    }

    public CalculateDeltaJobConfig(DataUnit newData, DataUnit oldData, String primaryJoinKey, String secondaryJoinKey,
            boolean filterPrimaryJoinKeyNulls, String workSpace) {
        this.setWorkspace(workSpace);
        this.newData = newData;
        this.oldData = oldData;
        this.primaryJoinKey = primaryJoinKey;
        this.secondaryJoinKey = secondaryJoinKey;
        this.filterPrimaryJoinKeyNulls = filterPrimaryJoinKeyNulls;
    }

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
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

    public Boolean getFilterPrimaryJoinKeyNulls() {
        return filterPrimaryJoinKeyNulls;
    }

    public String getSecondaryJoinKey() {
        return secondaryJoinKey;
    }

    public void setSecondaryJoinKey(String secondaryJoinKey) {
        this.secondaryJoinKey = secondaryJoinKey;
    }

    public void setFilterPrimaryJoinKeyNulls(Boolean filterPrimaryJoinKeyNulls) {
        this.filterPrimaryJoinKeyNulls = filterPrimaryJoinKeyNulls;
    }

    public String getPrimaryJoinKey() {
        return primaryJoinKey;
    }

    public void setPrimaryJoinKey(String primaryJoinKey) {
        this.primaryJoinKey = primaryJoinKey;
    }
}
