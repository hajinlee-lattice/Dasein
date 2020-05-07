package com.latticeengines.domain.exposed.dcp;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DCPImportRequest {

    @JsonProperty("project_id")
    private String projectId;

    @JsonProperty("source_id")
    private String sourceId;

    @JsonProperty("s3_file_key")
    private String s3FileKey;

    @JsonProperty("source_file_name")
    private String sourceFileName;

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

    public String getSourceFileName() {
        return sourceFileName;
    }

    public void setSourceFileName(String sourceFileName) {
        this.sourceFileName = sourceFileName;
    }
}
