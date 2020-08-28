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

    @JsonProperty("projectId")
    private String projectId;

    @JsonProperty("projectDisplayName")
    private String projectDisplayName;

    @JsonProperty("projectDescription")
    private String projectDescription;

    @JsonProperty("archived")
    private Boolean archived;

    @JsonProperty("sources")
    private List<Source> sources;

    @JsonProperty("totalSourceCount")
    private long totalSourceCount;

    @JsonProperty("basicStats")
    private DataReport.BasicStats basicStats;

    @JsonProperty("recipientList")
    private List<String> recipientList;

    @JsonProperty("created")
    private Long created;

    @JsonProperty("updated")
    private Long updated;

    @JsonProperty("createdBy")
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

    public String getProjectDescription() {
        return projectDescription;
    }

    public void setProjectDescription(String projectDescription) {
        this.projectDescription = projectDescription;
    }

    public Boolean getArchived() {
        return archived;
    }

    public void setArchived(Boolean archived) {
        this.archived = archived;
    }

    public List<Source> getSources() {
        return sources;
    }

    public void setSources(List<Source> sources) {
        this.sources = sources;
    }

    public long getTotalSourceCount() {
        return totalSourceCount;
    }

    public void setTotalSourceCount(long totalSourceCount) {
        this.totalSourceCount = totalSourceCount;
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
