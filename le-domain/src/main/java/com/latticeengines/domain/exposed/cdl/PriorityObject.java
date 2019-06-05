package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PriorityObject implements Comparable<PriorityObject> {

    @JsonProperty("tenant_name")
    private String tenantName;

    @JsonProperty("data_cloud_refresh")
    private int dataCloudRefresh;

    @JsonProperty("schedule_now")
    private int scheduleNow;

    @JsonProperty("schedule_time")
    private Long scheduleTime;

    @JsonProperty("first_action_time")
    private Long firstActionTime;

    @JsonProperty("invoke_time")
    private Long invokeTime;

    @JsonProperty("retry")
    private int retry;

    @Override
    public int compareTo(PriorityObject o) {
        long middleResult = o.getScheduleNow() - this.scheduleNow;
        long retryResult = o.getRetry() - this.retry;
        if (middleResult != 0) {
            return middleResult > 0 ? 1 : -1;
        } else {
            if (retryResult != 0) {
                return retryResult > 0 ? 1 : -1;
            }
            if (scheduleNow == 1) {
                middleResult = this.scheduleTime - o.getScheduleTime();
                return middleResult < 0 ? -1 : 1;
            } else {
                middleResult = this.invokeTime - o.getInvokeTime();
                if (middleResult != 0) {
                    return middleResult < 0 ? -1 : 1;
                } else {
                    if (this.invokeTime != 0) {
                        middleResult = this.firstActionTime - o.getFirstActionTime();
                        return middleResult < 0 ? -1 : 1;
                    } else {//no scheduleNow, no invokeTime, judge dataCloudRefresh
                        middleResult = o.getDataCloudRefresh() - this.dataCloudRefresh;
                        if (middleResult != 0) {
                            return middleResult < 0 ? -1 : 1;
                        } else {//judge the firstActionTime
                            middleResult = this.firstActionTime - o.getFirstActionTime();
                            return middleResult < 0 ? -1 : 1;
                        }
                    }
                }
            }
        }
    }

    public boolean objectEquals(PriorityObject o) {
        if (this.tenantName.equals(o.getTenantName())) {
            Long result =
                    this.firstActionTime - o.getFirstActionTime() +
                            this.scheduleNow - o.getScheduleNow() +
                            this.scheduleTime - o.getScheduleTime() +
                            this.invokeTime - o.getInvokeTime() +
                            this.dataCloudRefresh - o.getDataCloudRefresh() +
                            this.retry - o.getRetry();
            return result == 0L;
        }
        return false;
    }

    public void update(PriorityObject o) {
        this.dataCloudRefresh = o.getDataCloudRefresh();
        this.invokeTime = o.getInvokeTime();
        this.scheduleTime = o.getScheduleTime();
        this.scheduleNow = o.getScheduleNow();
        this.firstActionTime = o.getFirstActionTime();
        this.retry = o.getRetry();
    }

    public int getDataCloudRefresh() {
        return dataCloudRefresh;
    }

    public void setDataCloudRefresh(int dataCloudRefresh) {
        this.dataCloudRefresh = dataCloudRefresh;
    }

    public int getScheduleNow() {
        return scheduleNow;
    }

    public void setScheduleNow(int scheduleNow) {
        this.scheduleNow = scheduleNow;
    }

    public Long getScheduleTime() {
        return scheduleTime;
    }

    public void setScheduleTime(Long scheduleTime) {
        this.scheduleTime = scheduleTime;
    }

    public Long getFirstActionTime() {
        return firstActionTime;
    }

    public void setFirstActionTime(Long firstActionTime) {
        this.firstActionTime = firstActionTime;
    }

    public Long getInvokeTime() {
        return invokeTime;
    }

    public void setInvokeTime(Long invokeTime) {
        this.invokeTime = invokeTime;
    }

    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }

    public String getTenantName() {
        return tenantName;
    }

    public int getRetry() {
        return retry;
    }

    public void setRetry(int retry) {
        this.retry = retry;
    }
}
