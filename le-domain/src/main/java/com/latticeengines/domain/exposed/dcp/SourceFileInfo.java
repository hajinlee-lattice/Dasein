package com.latticeengines.domain.exposed.dcp;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class SourceFileInfo {

    @JsonProperty("file_import_id")
    private String fileImportId;

    @JsonProperty("display_name")
    private String displayName;

    @JsonProperty("file_rows")
    private Long fileRows;

    public String getFileImportId() {
        return fileImportId;
    }

    public void setFileImportId(String fileImportId) {
        this.fileImportId = fileImportId;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public Long getFileRows() {
        return fileRows;
    }

    public void setFileRows(Long fileRows) {
        this.fileRows = fileRows;
    }
}
