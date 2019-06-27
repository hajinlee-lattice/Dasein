package com.latticeengines.domain.exposed.cdl.scheduling;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.security.TenantType;

public class TenantActivity {

    @JsonProperty("tenant_id")
    private String tenantId;

    @JsonProperty("tenant_type")
    private TenantType tenantType;

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

    @JsonProperty("is_auto_schedule")
    private boolean isAutoSchedule;

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public TenantType getTenantType() {
        return tenantType;
    }

    public void setTenantType(TenantType tenantType) {
        this.tenantType = tenantType;
    }

    public boolean isLarge() {
        return isLarge;
    }

    public void setLarge(boolean large) {
        isLarge = large;
    }

    public boolean isScheduledNow() {
        return scheduledNow;
    }

    public void setScheduledNow(boolean scheduledNow) {
        this.scheduledNow = scheduledNow;
    }

    public Long getScheduleTime() {
        return scheduleTime;
    }

    public void setScheduleTime(Long scheduleTime) {
        this.scheduleTime = scheduleTime;
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

    public Long getLastFinishTime() {
        return lastFinishTime;
    }

    public void setLastFinishTime(Long lastFinishTime) {
        this.lastFinishTime = lastFinishTime;
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

    public boolean isAutoSchedule() {
        return isAutoSchedule;
    }

    public void setAutoSchedule(boolean autoSchedule) {
        isAutoSchedule = autoSchedule;
    }
}
