package com.latticeengines.domain.exposed.datacloud.ingestion;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class S3Destination {

    @JsonProperty("S3Bucket")
    private String s3Bucket;

    @JsonProperty("SourceName")
    private String sourceName;

    @JsonProperty("SourceVersion")
    private String sourceVersion;

    @JsonProperty("UpdateCurrentVersion")
    private Boolean updateCurrentVersion;

    public String getS3Bucket() {
        return s3Bucket;
    }

    public void setS3Bucket(String s3Bucket) {
        this.s3Bucket = s3Bucket;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public String getSourceVersion() {
        return sourceVersion;
    }

    public void setSourceVersion(String sourceVersion) {
        this.sourceVersion = sourceVersion;
    }

    public Boolean getUpdateCurrentVersion() {
        return updateCurrentVersion;
    }

    public void setUpdateCurrentVersion(Boolean updateCurrentVersion) {
        this.updateCurrentVersion = updateCurrentVersion;
    }
}
