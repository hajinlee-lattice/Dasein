package com.latticeengines.domain.exposed.datacloud.ingestion;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Usually API based data provider provides 2 API, one is to get current version
 * information (currently only support timestamp based version), the other is to
 * deliver file.
 *
 * Keep doc
 * https://confluence.lattice-engines.com/display/ENG/DataCloud+Engine+Architecture#DataCloudEngineArchitecture-Ingestion
 * up to date if there is any new change
 */
public class ApiConfiguration extends ProviderConfiguration {
    // API to get current version info
    private String versionUrl;
    // Timestamp format
    private String versionFormat;
    // API to get file
    private String fileUrl;
    // Filename (currently only support static file name)
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
}
