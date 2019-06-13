package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.security.TenantType;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "name")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ScheduleNowTenantActivity.class, name = "CustomerPriorityObject"),
        @JsonSubTypes.Type(value = AutoScheduleTenantActivity.class, name = "AutoScheduleTenantActivity"),
        @JsonSubTypes.Type(value = DataCloudRefreshTenantActivity.class, name = "DataCloudRefreshTenantActivity"),
})
public class TenantActivity implements Comparable<TenantActivity> {

    @JsonProperty("tenant_id")
    private String tenantId;

    @JsonProperty("tenant_type")
    protected TenantType tenantType;

    @JsonProperty("is_large")
    private boolean isLarge;

    @JsonProperty("scheduled_now")
    private boolean scheduledNow;

    @JsonProperty("schedule_time")
    private Long scheduleTime;

    @JsonProperty("invoke_time")
    private Long invokeTime;

    @JsonProperty("last_action_time")
    private Long lastActionTime;

    @JsonProperty("first_action_time")
    private Long firstActionTime;

    @JsonProperty("last_finish_time")
    private Long lastFinishTime;

    @JsonProperty("is_retry")
    private boolean isRetry;

    @JsonProperty("is_data_cloud_refresh")
    private boolean isDataCloudRefresh;

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

    public boolean isScheduledNow() {
        return scheduledNow;
    }

    public void setScheduledNow(boolean scheduledNow) {
        this.scheduledNow = scheduledNow;
    }

    public Long getInvokeTime() {
        return invokeTime;
    }

    public void setInvokeTime(Long invokeTime) {
        this.invokeTime = invokeTime;
    }

    public Long getLastActionTime() {
        return lastActionTime;
    }

    public void setLastActionTime(Long lastActionTime) {
        this.lastActionTime = lastActionTime;
    }

    public Long getFirstActionTime() {
        return firstActionTime;
    }

    public void setFirstActionTime(Long firstActionTime) {
        this.firstActionTime = firstActionTime;
    }

    public boolean isRetry() {
        return isRetry;
    }

    public void setRetry(boolean retry) {
        isRetry = retry;
    }

    public boolean isDataCloudRefresh() {
        return isDataCloudRefresh;
    }

    public void setDataCloudRefresh(boolean dataCloudRefresh) {
        isDataCloudRefresh = dataCloudRefresh;
    }

    public Long getScheduleTime() {
        return scheduleTime;
    }

    public void setScheduleTime(Long scheduleTime) {
        this.scheduleTime = scheduleTime;
    }

    @Override
    public int compareTo(TenantActivity o) {
        if (o.getTenantType() == tenantType) {
            return 0;
        }
        return o.getTenantType() == TenantType.CUSTOMER ? 1 : -1;
    }

    public Long getLastFinishTime() {
        return lastFinishTime;
    }

    public void setLastFinishTime(Long lastFinishTime) {
        this.lastFinishTime = lastFinishTime;
    }
}
