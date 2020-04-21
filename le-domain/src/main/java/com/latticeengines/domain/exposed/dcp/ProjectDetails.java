package com.latticeengines.domain.exposed.dcp;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ProjectDetails {

    @JsonProperty("project_id")
    private String projectId;

    @JsonProperty("project_display_name")
    private String projectDisplayName;

    @JsonProperty("project_root_path")
    private String projectRootPath;

    @JsonProperty("drop_folder_access")
    private GrantDropBoxAccessResponse dropFolderAccess;

    @JsonProperty("archived")
    private Boolean deleted;

    @JsonProperty("sources")
    private List<Source> sources;

    @JsonProperty("recipient_list")
    private List<String> recipientList;

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

    public GrantDropBoxAccessResponse getDropFolderAccess() {
        return dropFolderAccess;
    }

    public void setDropFolderAccess(GrantDropBoxAccessResponse dropFolderAccess) {
        this.dropFolderAccess = dropFolderAccess;
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

    public List<String> getRecipientList() {
        return recipientList;
    }

    public void setRecipientList(List<String> recipientList) {
        this.recipientList = recipientList;
    }
}
