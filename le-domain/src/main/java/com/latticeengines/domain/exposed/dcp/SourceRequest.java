package com.latticeengines.domain.exposed.dcp;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;

import io.swagger.annotations.ApiModelProperty;

public class SourceRequest {

    @JsonProperty("display_name")
    @ApiModelProperty(required = true, value = "displayName")
    private String displayName;

    @JsonProperty("project_id")
    @ApiModelProperty(required = true, value = "projectId")
    private String projectId;

    @JsonProperty("source_id")
    @ApiModelProperty(value = "sourceId")
    private String sourceId;

    @JsonProperty("simple_template_metadata")
    @ApiModelProperty(value = "simpleTemplateMetadata")
    private SimpleTemplateMetadata simpleTemplateMetadata;

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

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public SimpleTemplateMetadata getSimpleTemplateMetadata() {
        return simpleTemplateMetadata;
    }

    public void setSimpleTemplateMetadata(SimpleTemplateMetadata simpleTemplateMetadata) {
        this.simpleTemplateMetadata = simpleTemplateMetadata;
    }
}
