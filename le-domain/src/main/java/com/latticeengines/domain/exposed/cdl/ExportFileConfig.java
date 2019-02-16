package com.latticeengines.domain.exposed.cdl;

public class ExportFileConfig {

    private String objectPath;

    private String bucketName;

    public ExportFileConfig() {
    }

    public ExportFileConfig(String objectPath, String bucketName) {
        this.objectPath = objectPath;
        this.bucketName = bucketName;
    }

    public String getObjectPath() {
        return objectPath;
    }

    public void setObjectPath(String objectPath) {
        this.objectPath = objectPath;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }
}
