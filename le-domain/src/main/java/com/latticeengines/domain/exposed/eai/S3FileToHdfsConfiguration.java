package com.latticeengines.domain.exposed.eai;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class S3FileToHdfsConfiguration extends CSVToHdfsConfiguration {

    private static final String INPUT_ROOT = "/atlas/rawinput";
    private static final String IN_PROGRESS = "/inprogress";
    private static final String COMPLETED = "/completed";
    private static final String SUCCEEDED = "/succeeded";
    private static final String FAILED = "/failed";

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

    @JsonIgnore
    public String getFailedPath() {

        if (!s3FilePath.contains(IN_PROGRESS)) {
            return StringUtils.EMPTY;
        }
        String[] parts = getParts(s3FilePath);
        return parts[0] + INPUT_ROOT + COMPLETED + FAILED + "/" + parts[4] + "/" + parts[5] + "/"  +
                getFileName(s3FilePath);
    }

    @JsonIgnore
    public String getSucceedPath() {
        if (!s3FilePath.contains(IN_PROGRESS)) {
            return StringUtils.EMPTY;
        }
        String[] parts = getParts(s3FilePath);
        return parts[0] + INPUT_ROOT + COMPLETED + SUCCEEDED + "/" + parts[4] + "/" + parts[5] + "/"  +
                getFileName(s3FilePath);
    }

    @JsonIgnore
    private String getFileName(String key) {
        if (StringUtils.isEmpty(key) || key.lastIndexOf('/') < 0) {
            return key;
        }
        return key.substring(key.lastIndexOf('/') + 1);
    }

    private String[] getParts(String key) {
        while (key.startsWith("/")) {
            key = key.substring(1);
        }
        return key.split("/");
    }

}
