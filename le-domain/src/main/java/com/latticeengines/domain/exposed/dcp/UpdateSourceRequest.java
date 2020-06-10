package com.latticeengines.domain.exposed.dcp;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;

import io.swagger.annotations.ApiModelProperty;

public class UpdateSourceRequest {
    @JsonProperty("source_id")
    @ApiModelProperty(required = true, value = "sourceId")
    private String sourceId;

    @JsonProperty("display_name")
    @ApiModelProperty(value = "displayName")
    private String displayName;

    @JsonProperty("file_import_id")
    @ApiModelProperty(value = "fileImportId")
    private String fileImportId;

    @JsonProperty("field_definitions_record")
    @ApiModelProperty(required = true, value = "fieldDefinitionsRecord")
    private FieldDefinitionsRecord fieldDefinitionsRecord;

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
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
