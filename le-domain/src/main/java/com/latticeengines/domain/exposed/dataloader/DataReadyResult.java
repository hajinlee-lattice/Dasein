package com.latticeengines.domain.exposed.dataloader;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DataReadyResult {

    @JsonProperty("success")
    private boolean success;

    @JsonProperty("error_message")
    private String errorMessage;

    @JsonProperty("data_ready")
    private boolean dataReady;

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public boolean isDataReady() {
        return dataReady;
    }

    public void setDataReady(boolean dataReady) {
        this.dataReady = dataReady;
    }
}
