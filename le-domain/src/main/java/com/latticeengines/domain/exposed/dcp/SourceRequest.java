package com.latticeengines.domain.exposed.dcp;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;

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

    @JsonProperty("file_import_id")
    @ApiModelProperty(value = "fileImportId")
    private String fileImportId;

    @JsonProperty("field_definitions_record")
    @ApiModelProperty(required = true, value = "fieldDefinitionsRecord")
    private FieldDefinitionsRecord fieldDefinitionsRecord;

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

    public String getFileImportId() {
        return fileImportId;
    }

    public void setFileImportId(String fileImportId) {
        this.fileImportId = fileImportId;
    }

    public FieldDefinitionsRecord getFieldDefinitionsRecord() {
        return fieldDefinitionsRecord;
    }

    public void setFieldDefinitionsRecord(FieldDefinitionsRecord fieldDefinitionsRecord) {
        this.fieldDefinitionsRecord = fieldDefinitionsRecord;
    }
}
