package com.latticeengines.domain.exposed.dataloader;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RunQueryResult {

    private boolean success;
    private String errorMessage;
    private String queryHandle;

    @JsonProperty("Success")
    public boolean getSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    @JsonProperty("ErrorMessage")
    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @JsonProperty("QueryHandle")
    public String getQueryHandle() {
        return queryHandle;
    }

    public void setQueryHandle(String queryHandle) {
        this.queryHandle = queryHandle;
    }
}
