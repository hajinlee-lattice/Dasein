package com.latticeengines.domain.exposed.dcp;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

@JsonInclude(Include.NON_NULL)
public class ProjectRequest {

    @JsonProperty("displayName")
    @ApiModelProperty(required = true, value = "displayName")
    private String displayName;

    @JsonProperty("projectId")
    @ApiModelProperty(value = "projectId")
    private String projectId;

    @JsonProperty("projectType")
    @ApiModelProperty(required = true, value = "projectType")
    private Project.ProjectType projectType;

    @JsonProperty("purposeOfUse")
    @ApiModelProperty(required = true, value = "purposeOfUse")
    private PurposeOfUse purposeOfUse;

    @JsonProperty("projectDescription")
    @ApiModelProperty(required = false, value = "projectDescription")
    private String projectDescription;

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public Project.ProjectType getProjectType() { return projectType; }

    public void setProjectType(Project.ProjectType projectType) { this.projectType = projectType; }

    public PurposeOfUse getPurposeOfUse() {
        return purposeOfUse;
    }

    public void setPurposeOfUse(PurposeOfUse purposeOfUse) {
        this.purposeOfUse = purposeOfUse;
    }

    public String getProjectDescription() {
        return projectDescription;
    }

    public void setProjectDescription(String projectDescription) {
        this.projectDescription = projectDescription;
    }
}
