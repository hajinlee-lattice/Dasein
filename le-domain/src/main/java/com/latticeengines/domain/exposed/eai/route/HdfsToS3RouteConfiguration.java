package com.latticeengines.domain.exposed.eai.route;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HdfsToS3RouteConfiguration extends CamelRouteConfiguration {

    public static final String OPEN_SUFFIX = "_EXPORTING_";

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

    @JsonProperty("hdfs_path")
    private String hdfsPath;

    @JsonProperty("target_filename")
    private String targetFilename;

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

    public String getHdfsPath() {
        return hdfsPath;
    }

    public void setHdfsPath(String hdfsPath) {
        this.hdfsPath = hdfsPath;
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
