package com.latticeengines.domain.exposed.eai;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class S3FileToHdfsConfiguration extends CSVToHdfsConfiguration {

    // AKA: TemplateName
    @JsonProperty("feed_type")
    private String feedType;

    @JsonProperty("s3_bucket")
    private String s3Bucket;

    @JsonProperty("s3_file_path")
    private String s3FilePath;

    public String getFeedType() {
        return feedType;
    }

    public void setFeedType(String feedType) {
        this.feedType = feedType;
    }

    public String getS3Bucket() {
        return s3Bucket;
    }

    public void setS3Bucket(String s3Bucket) {
        this.s3Bucket = s3Bucket;
    }

    public String getS3FilePath() {
        return s3FilePath;
    }

    public void setS3FilePath(String s3FilePath) {
        this.s3FilePath = s3FilePath;
    }

    @JsonIgnore
    public String getS3FileName() {
        if (StringUtils.isEmpty(s3FilePath) || s3FilePath.lastIndexOf('/') < 0) {
            return s3FilePath;
        }
        return s3FilePath.substring(s3FilePath.lastIndexOf('/') + 1);
    }

}
