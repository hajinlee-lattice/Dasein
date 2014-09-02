package com.latticeengines.domain.exposed.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SubmissionResult {

    private boolean successful;

    public SubmissionResult() {
    }

    public SubmissionResult(boolean successful) {
    	this.successful = successful;
    }

    @JsonProperty("success")
    public boolean isSuccessful() {
        return successful;
    }

    @JsonProperty("success")
    public void setSuccessful(boolean successful) {
    	this.successful = successful;
    }
}
