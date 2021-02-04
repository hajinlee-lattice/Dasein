package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DataOperationRequest {

    @JsonProperty("s3Bucket")
    @ApiModelProperty(required = true, value = "s3Bucket")
    private String s3Bucket;

    @JsonProperty("s3DropPath")
    @ApiModelProperty(required = true, value = "s3DropPath")
    private String s3DropPath;

    @JsonProperty("s3FileKey")
    @ApiModelProperty(value = "s3FileKey")
    private String s3FileKey;

    public String getS3Bucket() {
        return s3Bucket;
    }

    public void setS3Bucket(String s3Bucket) {
        this.s3Bucket = s3Bucket;
    }

    public String getS3DropPath() {
        return s3DropPath;
    }

    public void setS3DropPath(String s3DropPath) {
        this.s3DropPath = s3DropPath;
    }

    public String getS3FileKey() {
        return s3FileKey;
    }

    public void setS3FileKey(String s3FileKey) {
        this.s3FileKey = s3FileKey;
    }

}
