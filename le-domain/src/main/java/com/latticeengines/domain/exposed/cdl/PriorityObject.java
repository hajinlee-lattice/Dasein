package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.security.TenantType;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "name")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ScheduleNowPriorityObject.class, name = "CustomerPriorityObject"),
        @JsonSubTypes.Type(value = AutoSchedulePriorityObject.class, name = "AutoSchedulePriorityObject"),
        @JsonSubTypes.Type(value = DataCloudRefreshPriorityObject.class, name = "DataCloudRefreshPriorityObject"),
})
public class PriorityObject implements Comparable<PriorityObject> {

    @JsonProperty("tenant_id")
    private String tenantId;

    @JsonProperty("tenant_type")
    protected TenantType tenantType;

    @JsonProperty("is_large")
    private boolean isLarge;

    @JsonProperty("scheduled_now")
    private boolean scheduledNow;

    public TenantType getTenantType() {
        return tenantType;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setIsLarge(boolean isLarge) {
        this.isLarge = isLarge;
    }

    public boolean isLarge() {
        return isLarge;
    }

    @Override
    public int compareTo(PriorityObject o) {
        if (o.getTenantType() == tenantType) {
            return 0;
        }
        return o.getTenantType() == TenantType.CUSTOMER ? 1 : -1;
    }

    public boolean isScheduledNow() {
        return scheduledNow;
    }

    public void setScheduledNow(boolean scheduledNow) {
        this.scheduledNow = scheduledNow;
    }
}
