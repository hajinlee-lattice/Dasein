package com.latticeengines.domain.exposed.dcp;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class UploadJobDetails {

    @JsonProperty("uploadId")
    private String uploadId;

    @JsonProperty("currentStep")
    private UploadJobStep currentStep;

    @JsonProperty("uploadJobSteps")
    private List<UploadJobStep> uploadJobSteps;

    @JsonProperty("progressPercentage")
    private String progressPercentage;

    @JsonProperty("uploadStats")
    public UploadStats statistics;

    @JsonProperty("uploadDiagnostics")
    public UploadDiagnostics uploadDiagnostics;

    public String getUploadId() {
        return uploadId;
    }

    public void setUploadId(String uploadId) {
        this.uploadId = uploadId;
    }

    public UploadJobStep getCurrentStep() {
        return currentStep;
    }

    public void setCurrentStep(UploadJobStep currentStep) {
        this.currentStep = currentStep;
    }

    public List<UploadJobStep> getUploadJobSteps() {
        return uploadJobSteps;
    }

    public void setUploadJobSteps(List<UploadJobStep> uploadJobSteps) {
        this.uploadJobSteps = uploadJobSteps;
    }

    public String getProgressPercentage() {
        return progressPercentage;
    }

    public void setProgressPercentage(String progressPercentage) {
        this.progressPercentage = progressPercentage;
    }

    public UploadStats getStatistics() {
        return statistics;
    }

    public void setStatistics(UploadStats statistics) {
        this.statistics = statistics;
    }

    public UploadDiagnostics getUploadDiagnostics() {
        return uploadDiagnostics;
    }

    public void setUploadDiagnostics(UploadDiagnostics uploadDiagnostics) {
        this.uploadDiagnostics = uploadDiagnostics;
    }

}
