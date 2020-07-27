package com.latticeengines.domain.exposed.dcp;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class UploadEmailInfo {

    @JsonProperty("upload_id")
    private String uploadId;

    @JsonProperty("source_id")
    private String sourceId;

    @JsonProperty("project_id")
    private String projectId;

    @JsonProperty("recipient_list")
    private List<String> recipientList;

    @JsonProperty("job_status")
    private String jobStatus;

    @JsonProperty("projectDisplayName")
    private String projectDisplayName;

    @JsonProperty("sourceDisplayName")
    private String sourceDisplayName;

    @JsonProperty("uploadDisplayName")
    private String uploadDisplayName;

    public String getUploadId() {
        return uploadId;
    }

    public void setUploadId(String uploadId) {
        this.uploadId = uploadId;
    }

    public String getUploadDisplayName() { return uploadDisplayName; }

    public void setUploadDisplayName(String uploadDisplayName) { this.uploadDisplayName = uploadDisplayName; }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getSourceDisplayName() { return sourceDisplayName; }

    public void setSourceDisplayName(String sourceDisplayName) { this.sourceDisplayName = sourceDisplayName; }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getProjectDisplayName() { return projectDisplayName; }

    public void setProjectDisplayName(String projectDisplayName) { this.projectDisplayName = projectDisplayName; }

    public List<String> getRecipientList() {
        return recipientList;
    }

    public void setRecipientList(List<String> recipientList) {
        this.recipientList = recipientList;
    }

    public String getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(String jobStatus) {
        this.jobStatus = jobStatus;
    }
}
