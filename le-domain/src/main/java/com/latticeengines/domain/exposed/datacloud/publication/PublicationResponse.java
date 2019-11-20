package com.latticeengines.domain.exposed.datacloud.publication;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.api.AppSubmission;

public class PublicationResponse {

    @JsonProperty("PublicationRequest")
    private PublicationRequest request;

    @JsonProperty("AppSubmissions")
    private AppSubmission appSubmissions;

    @JsonProperty("Message")
    private String message;

    public PublicationResponse() {

    }

    public PublicationResponse(PublicationRequest request, AppSubmission appSubmissions, String message) {
        this.request = request;
        this.appSubmissions = appSubmissions;
        this.message = message;
    }

    public PublicationRequest getRequest() {
        return request;
    }

    public void setRequest(PublicationRequest request) {
        this.request = request;
    }

    public AppSubmission getAppSubmissions() {
        return appSubmissions;
    }

    public void setAppSubmissions(AppSubmission appSubmissions) {
        this.appSubmissions = appSubmissions;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

}
