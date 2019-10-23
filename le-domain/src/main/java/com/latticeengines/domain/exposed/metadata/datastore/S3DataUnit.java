package com.latticeengines.domain.exposed.metadata.datastore;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonProperty;

public class S3DataUnit extends DataUnit {

    @Deprecated
    @JsonProperty("LinkedDir")
    private String linkedDir;

    @JsonProperty("Bucket")
    private String bucket;

    @JsonProperty("Prefix")
    private String prefix;

    @Override
    @JsonProperty("StorageType")
    public StorageType getStorageType() {
        return StorageType.S3;
    }

    public String getFullPath(String protocol) {
        String protocolStart = protocol.endsWith("://") ? "" : "://";
        String slash = "/";
        StringBuffer fullPath = new StringBuffer();
        fixBucketAndPrefix();
        if (StringUtils.isNotEmpty(bucket)) {
            fullPath.append(protocol).append(protocolStart).append(bucket).append(slash).append(prefix);
            return fullPath.toString();
        } else {
            throw new RuntimeException("Invalid link dir, can't get full path.");
        }
    }

    public String getLinkedDir() {
        return linkedDir;
    }

    public void setLinkedDir(String linkedDir) {
        this.linkedDir = linkedDir;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public void fixBucketAndPrefix() {
        if (StringUtils.isNotEmpty(linkedDir)) {
            Matcher matcher = Pattern.compile("^(s3a|s3n|s3)://(?<bucket>[^/]+)/(?<prefix>.*)").matcher(linkedDir);
            if (matcher.matches()) {
                String bucket = matcher.group("bucket");
                String prefix = matcher.group("prefix");
                this.bucket = bucket;
                // if prefix end with a file, then extract path
                if (Pattern.compile(".*/.*\\..*$").matcher(prefix).matches()) {
                    prefix = prefix.substring(0, prefix.lastIndexOf("/"));
                }
                this.prefix = prefix;
            }
        }
    }
}
