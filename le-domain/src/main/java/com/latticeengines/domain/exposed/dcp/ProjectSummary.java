package com.latticeengines.domain.exposed.dcp;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ProjectSummary {

    @JsonProperty("project_id")
    private String projectId;

    @JsonProperty("project_display_name")
    private String projectDisplayName;

    @JsonProperty("archived")
    private Boolean archieved;

    @JsonProperty("basic_stats")
    private DataReport.BasicStats basicStats;

    @JsonProperty("recipient_list")
    private List<String> recipientList;

    @JsonProperty("created")
    private Long created;

    @JsonProperty("updated")
    private Long updated;

    @JsonProperty("created_by")
    private String createdBy;

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getProjectDisplayName() {
        return projectDisplayName;
    }

    public void setProjectDisplayName(String projectDisplayName) {
        this.projectDisplayName = projectDisplayName;
    }

    public Boolean getArchieved() {
        return archieved;
    }

    public void setArchieved(Boolean archieved) {
        this.archieved = archieved;
    }

    public DataReport.BasicStats getBasicStats() {
        return basicStats;
    }

    public void setBasicStats(DataReport.BasicStats basicStats) {
        this.basicStats = basicStats;
    }

    public List<String> getRecipientList() {
        return recipientList;
    }

    public void setRecipientList(List<String> recipientList) {
        this.recipientList = recipientList;
    }

    public Long getCreated() {
        return created;
    }

    public void setCreated(Long created) {
        this.created = created;
    }

    public Long getUpdated() {
        return updated;
    }

    public void setUpdated(Long updated) {
        this.updated = updated;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }
}
