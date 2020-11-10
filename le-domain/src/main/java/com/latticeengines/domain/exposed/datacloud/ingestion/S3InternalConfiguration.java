package com.latticeengines.domain.exposed.datacloud.ingestion;

import com.fasterxml.jackson.annotation.JsonProperty;

public class S3InternalConfiguration extends ProviderConfiguration {
    @JsonProperty("SourceBucket")
    private String sourceBucket;

    @JsonProperty("ParentDir")
    private String parentDir;

    @JsonProperty("SubfolderDateFormat")
    private String subfolderDateFormat;

    // Source name on hdfs. By default, data will be ingested into hdfs
    @JsonProperty("SourceNameOnHdfs")
    private String sourceNameOnHdfs;

    @JsonProperty("CheckControlFile")
    private Boolean checkControlFile;

    @JsonProperty("FileExtension")
    private String fileExtension;

    @JsonProperty("HasSubFolder")
    private Boolean hasSubFolder;

    public String getSourceBucket() {
        return sourceBucket;
    }

    public void setSourceBucket(String sourceBucket) {
        this.sourceBucket = sourceBucket;
    }

    public String getParentDir() {
        return parentDir;
    }

    public void setParentDir(String parentDir) {
        this.parentDir = parentDir;
    }

    public String getSubfolderDateFormat() {
        return subfolderDateFormat;
    }

    public void setSubfolderDateFormat(String subfolderDateFormat) {
        this.subfolderDateFormat = subfolderDateFormat;
    }

    public String getSourceNameOnHdfs() {
        return sourceNameOnHdfs;
    }

    public void setSourceNameOnHdfs(String sourceNameOnHdfs) {
        this.sourceNameOnHdfs = sourceNameOnHdfs;
    }

    public Boolean getCheckControlFile() {
        return checkControlFile;
    }

    public String getFileExtension() {
        return fileExtension;
    }

    public Boolean getHasSubFolder() {
        return hasSubFolder;
    }

    public void setHasSubFolder(Boolean hasSubFolder) {
        this.hasSubFolder = hasSubFolder;
    }
}
