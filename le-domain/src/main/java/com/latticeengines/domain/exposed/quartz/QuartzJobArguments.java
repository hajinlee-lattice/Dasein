package com.latticeengines.domain.exposed.quartz;

import com.fasterxml.jackson.annotation.JsonProperty;

public class QuartzJobArguments {

    @JsonProperty("job_name")
    private String jobName;

    @JsonProperty("tenant_id")
    private String tenantId;

    @JsonProperty("predefined_job_type")
    private String predefinedJobType;

    @JsonProperty("job_arguments")
    private String jobArguments;

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

    public String getPredefinedJobType() {
        return predefinedJobType;
    }

    public void setPredefinedJobType(String predefinedJobType) {
        this.predefinedJobType = predefinedJobType;
    }

    public String getJobArguments() {
        return jobArguments;
    }

    public void setJobArguments(String jobArguments) {
        this.jobArguments = jobArguments;
    }

}
