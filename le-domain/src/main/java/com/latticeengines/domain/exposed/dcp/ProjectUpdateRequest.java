package com.latticeengines.domain.exposed.dcp;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ProjectUpdateRequest {

    @JsonProperty("displayName")
    @ApiModelProperty(value = "displayName")
    private String displayName;

    @JsonProperty("projectDescription")
    @ApiModelProperty(value = "projectDescription")
    private String projectDescription;

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getProjectDescription() {
        return projectDescription;
    }

    public void setProjectDescription(String projectDescription) {
        this.projectDescription = projectDescription;
    }
}
