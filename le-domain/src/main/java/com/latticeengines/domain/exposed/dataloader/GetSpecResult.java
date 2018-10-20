package com.latticeengines.domain.exposed.dataloader;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GetSpecResult {
    private String errorMessage;

    private String success;

    private String specDetails;

    @JsonProperty("ErrorMessage")
    public String getErrorMessage() {
        return this.errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @JsonProperty("Success")
    public String getSuccess() {
        return this.success;
    }

    public void setSuccess(String success) {
        this.success = success;
    }

    @JsonProperty("SpecDetails")
    public String getSpecDetails() {
        return specDetails;
    }

    public void setSpecDetails(String specDetails) {
        this.specDetails = specDetails;
    }

}
