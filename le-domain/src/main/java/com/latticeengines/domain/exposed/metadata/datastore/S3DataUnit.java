package com.latticeengines.domain.exposed.metadata.datastore;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.util.S3PathBuilder;

public class S3DataUnit extends DataUnit {

    @Deprecated
    @JsonProperty("LinkedDir")
    private String linkedDir;

    @JsonProperty("BucketName")
    private String bucketName;

    @JsonProperty("Prefix")
    private String prefix;

    @Override
    @JsonProperty("StorageType")
    public StorageType getStorageType() {
        return StorageType.S3;
    }

    public String getFullPath(String protocol) {
        String protocolStart = "://";
        String slash = "/";
        StringBuffer fullPath = new StringBuffer();
        if (StringUtils.isNotEmpty(bucketName)) {
            fullPath.append(protocol).append(protocolStart).append(bucketName).append(slash).append(prefix);
            return fullPath.toString();
        } else {
            // use old link dir to construct full path
            S3PathBuilder.setS3Bucket(this);
            if (StringUtils.isNotEmpty(bucketName)) {
                fullPath.append(protocol).append(protocolStart).append(bucketName).append(slash).append(prefix);
                return fullPath.toString();
            } else {
                throw new RuntimeException("Invalid link dir, can't get full path.");
            }
        }
    }

    public String getLinkedDir() {
        return linkedDir;
    }

    public void setLinkedDir(String linkedDir) {
        this.linkedDir = linkedDir;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
}
