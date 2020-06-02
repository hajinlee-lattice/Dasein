package com.latticeengines.domain.exposed.dcp;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DCPImportRequest {

    @JsonProperty("project_id")
    @ApiModelProperty(required = true, value = "projectId")
    private String projectId;

    @JsonProperty("source_id")
    @ApiModelProperty(required = true, value = "sourceId")
    private String sourceId;

    @JsonProperty("s3_file_key")
    @ApiModelProperty(required = false, value = "s3FileKey")
    private String s3FileKey;

    @JsonProperty("file_import_id")
    @ApiModelProperty(required = false, value = "fileImportId")
    private String fileImportId;

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

    public String getS3FileKey() {
        return s3FileKey;
    }

    public void setS3FileKey(String s3FileKey) {
        this.s3FileKey = s3FileKey;
    }

    public String getFileImportId() {
        return fileImportId;
    }

    public void setFileImportId(String fileImportId) {
        this.fileImportId = fileImportId;
    }
}
