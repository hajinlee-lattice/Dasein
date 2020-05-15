package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SystemStatus {

    @JsonProperty("can_run_job_count")
    private int canRunJobCount;

    @JsonProperty("can_run_large_job_count")
    private int canRunLargeJobCount;

    // tenant with large txn have an additional quota
    @JsonProperty("can_run_large_txn_job_count")
    private int canRunLargeTxnJobCount;

    @JsonProperty("can_run_schedule_now_job_count")
    private int canRunScheduleNowJobCount;

    @JsonProperty("running_total_count")
    private int runningTotalCount;

    @JsonProperty("running_schedule_now_count")
    private int runningScheduleNowCount;

    @JsonProperty("running_large_job_count")
    private int runningLargeJobCount;

    @JsonProperty("running_large_txn_job_count")
    private int runningLargeTxnJobCount;

    @JsonProperty("large_job_tenant_id")
    private Set<String> largeJobTenantId;

    @JsonProperty("large_transaction_tenant_id")
    private Set<String> largeTransactionTenantId;

    @JsonProperty("running_pa_tenant_id")
    private  Set<String> runningPATenantId;

    @JsonProperty("schedule_tenants")
    private Set<String> scheduleTenants;//set of tenants that we already decide to schedule job.


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

    public int getCanRunLargeTxnJobCount() {
        return canRunLargeTxnJobCount;
    }

    public void setCanRunLargeTxnJobCount(int canRunLargeTxnJobCount) {
        this.canRunLargeTxnJobCount = canRunLargeTxnJobCount;
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

    public int getRunningLargeTxnJobCount() {
        return runningLargeTxnJobCount;
    }

    public void setRunningLargeTxnJobCount(int runningLargeTxnJobCount) {
        this.runningLargeTxnJobCount = runningLargeTxnJobCount;
    }

    public Set<String> getLargeJobTenantId() {
        return largeJobTenantId;
    }

    public void setLargeJobTenantId(Set<String> largeJobTenantId) {
        this.largeJobTenantId = largeJobTenantId;
    }

    public Set<String> getLargeTransactionTenantId() {
        return largeTransactionTenantId;
    }

    public void setLargeTransactionTenantId(Set<String> largeTransactionTenantId) {
        this.largeTransactionTenantId = largeTransactionTenantId;
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
        this.runningTotalCount = this.runningTotalCount + 1;
        if (tenantActivity.isLarge() || tenantActivity.isLargeTransaction()) {
            this.canRunLargeJobCount = this.canRunLargeJobCount - 1;
            this.runningLargeJobCount = this.runningLargeJobCount + 1;
        }
        if (tenantActivity.isLargeTransaction()) {
            this.canRunLargeTxnJobCount--;
            this.runningLargeTxnJobCount++;
        }
        if (tenantActivity.isScheduledNow()) {
            this.canRunScheduleNowJobCount = this.canRunScheduleNowJobCount - 1;
            this.runningScheduleNowCount = this.runningScheduleNowCount + 1;
        }
    }

    /**
     * will change canRunJobCount when PA finished using for simulation
     *
     */
    public void changeSystemStateAfterPAFinished(TenantActivity tenantActivity) {
        this.canRunJobCount = this.canRunJobCount + 1;
        this.runningTotalCount = this.runningTotalCount - 1;
        if (tenantActivity.isLarge() || tenantActivity.isLargeTransaction()) {
            this.canRunLargeJobCount = this.canRunLargeJobCount + 1;
            this.runningLargeJobCount = this.runningLargeJobCount - 1;
        }
        if (tenantActivity.isLargeTransaction()) {
            this.canRunLargeTxnJobCount++;
            this.runningLargeTxnJobCount--;
        }
        if (tenantActivity.isScheduledNow()) {
            this.canRunScheduleNowJobCount = this.canRunScheduleNowJobCount + 1;
            this.runningScheduleNowCount = this.runningScheduleNowCount - 1;
        }
    }

    public Set<String> getScheduleTenants() {
        if (scheduleTenants == null) {
            scheduleTenants = new HashSet<>();
        }
        return scheduleTenants;
    }

    public void setScheduleTenants(Set<String> scheduleTenants) {
        this.scheduleTenants = scheduleTenants;
    }

    @Override
    public String toString() {
        return "SystemStatus{" + "canRunJobCount=" + canRunJobCount + ", canRunLargeJobCount=" + canRunLargeJobCount
                + ", canRunLargeTxnJobCount=" + canRunLargeTxnJobCount + ", canRunScheduleNowJobCount="
                + canRunScheduleNowJobCount + ", runningTotalCount=" + runningTotalCount + ", runningScheduleNowCount="
                + runningScheduleNowCount + ", runningLargeJobCount=" + runningLargeJobCount
                + ", runningLargeTxnJobCount=" + runningLargeTxnJobCount + ", largeJobTenantId=" + largeJobTenantId
                + ", largeTransactionTenantId=" + largeTransactionTenantId + ", runningPATenantId=" + runningPATenantId
                + ", scheduleTenants=" + scheduleTenants + '}';
    }
}
