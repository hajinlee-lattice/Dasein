package com.latticeengines.domain.exposed.dcp;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;

import io.swagger.annotations.ApiModelProperty;

public class UpdateSourceRequest {

    @JsonProperty("display_name")
    @ApiModelProperty(value = "displayName")
    private String displayName;

    @JsonProperty("import_file")
    @ApiModelProperty(value = "importFile")
    private String importFile;

    @JsonProperty("field_definitions_record")
    @ApiModelProperty(required = true, value = "fieldDefinitionsRecord")
    private FieldDefinitionsRecord fieldDefinitionsRecord;

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getImportFile() {
        return importFile;
    }

    public void setImportFile(String importFile) {
        this.importFile = importFile;
    }

    public FieldDefinitionsRecord getFieldDefinitionsRecord() {
        return fieldDefinitionsRecord;
    }

    public void setFieldDefinitionsRecord(FieldDefinitionsRecord fieldDefinitionsRecord) {
        this.fieldDefinitionsRecord = fieldDefinitionsRecord;
    }
}
