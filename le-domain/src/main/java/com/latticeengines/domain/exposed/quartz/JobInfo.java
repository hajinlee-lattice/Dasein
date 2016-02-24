package com.latticeengines.domain.exposed.quartz;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JobInfo {

    @JsonProperty("job_name")
    private String jobName;

    @JsonProperty("tenant_id")
    private String tenantId;

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

}
