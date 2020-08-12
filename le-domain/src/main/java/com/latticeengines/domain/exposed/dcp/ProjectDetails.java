package com.latticeengines.domain.exposed.dcp;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ProjectDetails {

    @JsonProperty("projectId")
    private String projectId;

    @JsonProperty("projectDisplayName")
    private String projectDisplayName;

    @JsonProperty("projectRelativePath")
    private String projectRootPath;

    @JsonProperty("projectFullPath")
    private String projectFullPath;

    @JsonProperty("archived")
    private Boolean deleted;

    @JsonProperty("sources")
    private List<Source> sources;

    @JsonProperty("totalSourceCount")
    private long totalSourceCount;

    @JsonProperty("recipientList")
    private List<String> recipientList;

    @JsonProperty("created")
    private Long created;

    @JsonProperty("updated")
    private Long updated;

    @JsonProperty("createdBy")
    private String createdBy;

    @JsonProperty("teamId")
    private String teamId;

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

    public String getProjectRootPath() {
        return projectRootPath;
    }

    public void setProjectRootPath(String projectRootPath) {
        this.projectRootPath = projectRootPath;
    }

    public String getProjectFullPath() {
        return projectFullPath;
    }

    public void setProjectFullPath(String projectFullPath) {
        this.projectFullPath = projectFullPath;
    }

    public Boolean getDeleted() {
        return deleted;
    }

    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
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

    public String getTeamId() {
        return teamId;
    }

    public void setTeamId(String teamId) {
        this.teamId = teamId;
    }
}
