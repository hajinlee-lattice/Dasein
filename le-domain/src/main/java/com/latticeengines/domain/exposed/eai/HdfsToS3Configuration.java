package com.latticeengines.domain.exposed.eai;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

@JsonIgnoreProperties(ignoreUnknown = true)
public class HdfsToS3Configuration extends ExportConfiguration {

    @JsonProperty("s3_bucket")
    private String s3Bucket;

    @JsonProperty("s3_prefix")
    private String s3Prefix;

    @JsonProperty("s3_region")
    private String s3Region = "us-east-1";

    // only applicable to avro source file
    // if this is sepcified, then the original file will be splited into small files with at most this size (Byte).
    // small files are easier to track progress, and better for loading to snowflake
    @JsonProperty("split_size")
    private Long splitSize;

    @JsonProperty("target_filename")
    @NotNull
    @NotEmptyString
    private String targetFilename; // the file name to be used in s3, may be suffixed by partition number if splitted.

    public String getS3Bucket() {
        return s3Bucket;
    }

    public void setS3Bucket(String s3Bucket) {
        this.s3Bucket = s3Bucket;
    }

    public String getS3Prefix() {
        return s3Prefix;
    }

    public void setS3Prefix(String s3Prefix) {
        this.s3Prefix = s3Prefix;
    }

    public String getS3Region() {
        return s3Region;
    }

    public void setS3Region(String s3Region) {
        this.s3Region = s3Region;
    }

    public String getTargetFilename() {
        return targetFilename;
    }

    public void setTargetFilename(String hdfsFilename) {
        this.targetFilename = hdfsFilename;
    }

    public Long getSplitSize() {
        return splitSize;
    }

    public void setSplitSize(Long splitSize) {
        this.splitSize = splitSize;
    }
}
