package com.latticeengines.domain.exposed.quartz;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JobInfoDetail {

    @JsonProperty("job_name")
    private String jobName;

    @JsonProperty("tenant_id")
    private String tenantId;

    @JsonProperty("next_trigger_time")
    private Date nextTriggerTime;

    @JsonProperty("history_jobs")
    private List<JobHistory> historyJobs;

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public Date getNextTriggerTime() {
        return nextTriggerTime;
    }

    public void setNextTriggerTime(Date nextTriggerTime) {
        this.nextTriggerTime = nextTriggerTime;
    }

    public List<JobHistory> getHistoryJobs() {
        return historyJobs;
    }

    public void setHistoryJobs(List<JobHistory> historyJobs) {
        this.historyJobs = historyJobs;
    }

}
