package com.latticeengines.domain.exposed.cdl;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SystemStatus {

    @JsonProperty("can_run_job_count")
    private int canRunJobCount;

    @JsonProperty("can_run_large_job_count")
    private int canRunLargeJobCount;

    @JsonProperty("can_run_schedule_now_job_count")
    private int canRunScheduleNowJobCount;

    @JsonProperty("running_total_count")
    private int runningTotalCount;

    @JsonProperty("running_schedule_now_count")
    private int runningScheduleNowCount;

    @JsonProperty("running_large_job_count")
    private int runningLargeJobCount;

    @JsonProperty("large_job_tenant_id")
    private Set<String> largeJobTenantId;

    @JsonProperty("running_pa_tenant_id")
    private  Set<String> runningPATenantId;


    public int getCanRunJobCount() {
        return canRunJobCount;
    }

    public void setCanRunJobCount(int canRunJobCount) {
        this.canRunJobCount = canRunJobCount;
    }

    public int getCanRunLargeJobCount() {
        return canRunLargeJobCount;
    }

    public void setCanRunLargeJobCount(int canRunLargeJobCount) {
        this.canRunLargeJobCount = canRunLargeJobCount;
    }

    public int getCanRunScheduleNowJobCount() {
        return canRunScheduleNowJobCount;
    }

    public void setCanRunScheduleNowJobCount(int canRunScheduleNowJobCount) {
        this.canRunScheduleNowJobCount = canRunScheduleNowJobCount;
    }

    public int getRunningTotalCount() {
        return runningTotalCount;
    }

    public void setRunningTotalCount(int runningTotalCount) {
        this.runningTotalCount = runningTotalCount;
    }

    public int getRunningScheduleNowCount() {
        return runningScheduleNowCount;
    }

    public void setRunningScheduleNowCount(int runningScheduleNowCount) {
        this.runningScheduleNowCount = runningScheduleNowCount;
    }

    public int getRunningLargeJobCount() {
        return runningLargeJobCount;
    }

    public void setRunningLargeJobCount(int runningLargeJobCount) {
        this.runningLargeJobCount = runningLargeJobCount;
    }

    public Set<String> getLargeJobTenantId() {
        return largeJobTenantId;
    }

    public void setLargeJobTenantId(Set<String> largeJobTenantId) {
        this.largeJobTenantId = largeJobTenantId;
    }

    public Set<String> getRunningPATenantId() {
        return runningPATenantId;
    }

    public void setRunningPATenantId(Set<String> runningPATenantId) {
        this.runningPATenantId = runningPATenantId;
    }

    /**
     *
     * Take tenantActivity to run PA editing the system status
     *
     */
    public void changeSystemState(TenantActivity tenantActivity) {
        this.canRunJobCount = this.canRunJobCount - 1;
        if (tenantActivity.isLarge()) {
            this.canRunLargeJobCount = this.canRunLargeJobCount - 1;
        }
        if (tenantActivity.isScheduledNow()) {
            this.canRunScheduleNowJobCount = this.canRunScheduleNowJobCount - 1;
        }
    }
}
