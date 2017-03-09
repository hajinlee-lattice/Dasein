package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;

public class VdbLoadTableStatus {

    @JsonProperty("vdb_query_handle")
    private String vdbQueryHandle;

    @JsonProperty("job_status")
    private String jobStatus;

    @JsonProperty("message")
    private String message;

    public String getVisiDBQueryHandle() {
        return vdbQueryHandle;
    }

    public void setVisiDBQueryHandle(String vdbQueryHandle) {
        this.vdbQueryHandle = vdbQueryHandle;
    }

    public String getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(String jobStatus) {
        this.jobStatus = jobStatus;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
