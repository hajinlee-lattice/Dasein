package com.latticeengines.domain.exposed.quartz;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TriggeredJobInfo {

    @JsonProperty("job_handle")
    private String jobHandle;

    @JsonProperty("execution_host")
    private String executionHost;

    public String getJobHandle() {
        return jobHandle;
    }

    public void setJobHandle(String jobHandle) {
        this.jobHandle = jobHandle;
    }

    public String getExecutionHost() {
        return executionHost;
    }

    public void setExecutionHost(String executionHost) {
        this.executionHost = executionHost;
    }

}
