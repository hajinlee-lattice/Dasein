package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class CalculateDeltaJobConfig extends SparkJobConfig {

    private static final long serialVersionUID = -3636956961979908155L;

    public static final String NAME = "calculateDelta";

    @JsonProperty("OldData")
    private DataUnit oldData;
    @JsonProperty("NewData")
    private DataUnit newData;
    @JsonProperty("FilterPrimaryJoinKeyNulls")
    private boolean filterPrimaryJoinKeyNulls = false;
    @JsonProperty("PrimaryJoinKey")
    private String primaryJoinKey;
    @JsonProperty("SecondaryJoinKey")
    private String secondaryJoinKey;
    @JsonProperty("IsAccountEntity")
    private boolean isAccountEntity = false;

    public CalculateDeltaJobConfig() {
    }

    public CalculateDeltaJobConfig(DataUnit newData, DataUnit oldData, String primaryJoinKey, String secondaryJoinKey,
                                   boolean filterPrimaryJoinKeyNulls, boolean isAccountEntity, String workSpace) {
        this.setWorkspace(workSpace);
        this.primaryJoinKey = primaryJoinKey;
        this.secondaryJoinKey = secondaryJoinKey;
        this.isAccountEntity = isAccountEntity;
        this.filterPrimaryJoinKeyNulls = filterPrimaryJoinKeyNulls;
        this.setInput(Lists.newArrayList(oldData, newData));
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

    public boolean getFilterPrimaryJoinKeyNulls() {
        return filterPrimaryJoinKeyNulls;
    }

    public String getSecondaryJoinKey() {
        return secondaryJoinKey;
    }

    public void setSecondaryJoinKey(String secondaryJoinKey) {
        this.secondaryJoinKey = secondaryJoinKey;
    }

    public void setFilterPrimaryJoinKeyNulls(boolean filterPrimaryJoinKeyNulls) {
        this.filterPrimaryJoinKeyNulls = filterPrimaryJoinKeyNulls;
    }

    public boolean getIsAccountEntity() {
        return isAccountEntity;
    }

    public void setIsAccountEntity(boolean isAccountEntity) {
        this.isAccountEntity = isAccountEntity;
    }

    public String getPrimaryJoinKey() {
        return primaryJoinKey;
    }

    public void setPrimaryJoinKey(String primaryJoinKey) {
        this.primaryJoinKey = primaryJoinKey;
    }
}
