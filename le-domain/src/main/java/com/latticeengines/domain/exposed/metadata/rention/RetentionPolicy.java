package com.latticeengines.domain.exposed.metadata.rention;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RetentionPolicy {

    @JsonProperty("NoExpire")
    private boolean noExpire = false;

    @JsonProperty("Count")
    private int count;

    private RetentionPolicyTimeUnit retentionPolicyTimeUnit;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public RetentionPolicyTimeUnit getRetentionPolicyTimeUnit() {
        return retentionPolicyTimeUnit;
    }

    public void setRetentionPolicyTimeUnit(RetentionPolicyTimeUnit retentionPolicyTimeUnit) {
        this.retentionPolicyTimeUnit = retentionPolicyTimeUnit;
    }

    public boolean isNoExpire() {
        return noExpire;
    }

    public void setNoExpire(boolean noExpire) {
        this.noExpire = noExpire;
    }

    @JsonProperty("RetentionPolicyTimeUnit")
    public void setRetentionPolicyTimeUnitByString(String retentionPolicyTimeUnitName) {
        setRetentionPolicyTimeUnit(RetentionPolicyTimeUnit.fromName(retentionPolicyTimeUnitName));
    }

    @JsonProperty("RetentionPolicyTimeUnit")
    public String getRetentionPolicyTimeUnitByString() {
        if (retentionPolicyTimeUnit != null) {
            return retentionPolicyTimeUnit.name();
        } else {
            return null;
        }
    }
}
