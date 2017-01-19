package com.latticeengines.domain.exposed.datacloud.ingestion;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ApiConfiguration extends ProviderConfiguration {
    private String versionUrl;
    private String versionFormat;
    private String fileUrl;
    private String fileName;

    @JsonProperty("VersionUrl")
    public String getVersionUrl() {
        return versionUrl;
    }

    @JsonProperty("VersionUrl")
    public void setVersionUrl(String versionUrl) {
        this.versionUrl = versionUrl;
    }

    @JsonProperty("VersionFormat")
    public String getVersionFormat() {
        return versionFormat;
    }

    @JsonProperty("VersionFormat")
    public void setVersionFormat(String versionFormat) {
        this.versionFormat = versionFormat;
    }

    @JsonProperty("FileUrl")
    public String getFileUrl() {
        return fileUrl;
    }

    @JsonProperty("FileUrl")
    public void setFileUrl(String fileUrl) {
        this.fileUrl = fileUrl;
    }

    @JsonProperty("FileName")
    public String getFileName() {
        return fileName;
    }

    @JsonProperty("FileName")
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public enum ApiType {
        GET, POST
    }
}
