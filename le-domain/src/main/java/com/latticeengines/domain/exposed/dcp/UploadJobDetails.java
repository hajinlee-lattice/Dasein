package com.latticeengines.domain.exposed.dcp;

import java.util.List;

import javax.persistence.EnumType;
import javax.persistence.Enumerated;

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

    @JsonProperty("displayName")
    private String displayName;

    @JsonProperty("sourceDisplayName")
    private String sourceDisplayName;

    @JsonProperty("status")
    @Enumerated(EnumType.STRING)
    private Upload.Status status;

    @JsonProperty("uploadStats")
    public UploadStats statistics;

    @JsonProperty("uploadDiagnostics")
    public UploadDiagnostics uploadDiagnostics;

    @JsonProperty("uploadJobSteps")
    private List<UploadJobStep> uploadJobSteps;

    @JsonProperty("dropFileTime")
    private Long dropFileTime;

    @JsonProperty("uploadCreatedTime")
    private Long uploadCreatedTime;

    @JsonProperty("currentStep")
    private UploadJobStep currentStep;

    @JsonProperty("progressPercentage")
    private Double progressPercentage;


    public String getUploadId() {
        return uploadId;
    }

    public void setUploadId(String uploadId) {
        this.uploadId = uploadId;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getSourceDisplayName() {
        return sourceDisplayName;
    }

    public void setSourceDisplayName(String sourceDisplayName) {
        this.sourceDisplayName = sourceDisplayName;
    }

    public Upload.Status getStatus() {
        return status;
    }
    public void setStatus(Upload.Status status) {
        this.status = status;
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

    public List<UploadJobStep> getUploadJobSteps() {
        return uploadJobSteps;
    }

    public void setUploadJobSteps(List<UploadJobStep> uploadJobSteps) {
        this.uploadJobSteps = uploadJobSteps;
    }

    public Long getDropFileTime() {
        return dropFileTime;
    }

    public void setDropFileTime(Long dropFileTime) {
        this.dropFileTime = dropFileTime;
    }

    public Long getUploadCreatedTime() {
        return uploadCreatedTime;
    }

    public void setUploadCreatedTime(Long uploadCreatedTime) {
        this.uploadCreatedTime = uploadCreatedTime;
    }

    public void setCurrentStep(UploadJobStep currentStep) {
        this.currentStep = currentStep;
    }

    public UploadJobStep getCurrentStep() {
        return currentStep;
    }

    public Double getProgressPercentage() {
        return progressPercentage;
    }

    public void setProgressPercentage(Double progressPercentage) {
        this.progressPercentage = progressPercentage;
    }
}
