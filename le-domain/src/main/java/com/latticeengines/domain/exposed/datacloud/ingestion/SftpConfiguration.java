package com.latticeengines.domain.exposed.datacloud.ingestion;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SftpConfiguration extends ProviderConfiguration {

    protected String sftpHost;
    protected Integer sftpPort;
    protected String sftpUserName;
    protected String sftpPasswordEncrypted;
    protected String sftpDir;
    protected Integer checkVersion;
    protected FileCheckStrategy checkStrategy;
    protected String fileExtension;
    protected String fileNamePrefix;
    protected String fileNamePostfix;
    protected String fileTimestamp;

    @JsonProperty("SftpHost")
    public String getSftpHost() {
        return sftpHost;
    }

    @JsonProperty("SftpHost")
    public void setSftpHost(String sftpHost) {
        this.sftpHost = sftpHost;
    }

    @JsonProperty("SftpPort")
    public Integer getSftpPort() {
        return sftpPort;
    }

    @JsonProperty("SftpPort")
    public void setSftpPort(Integer sftpPort) {
        this.sftpPort = sftpPort;
    }

    @JsonProperty("SftpUsername")
    public String getSftpUserName() {
        return sftpUserName;
    }

    @JsonProperty("SftpUsername")
    public void setSftpUserName(String sftpUserName) {
        this.sftpUserName = sftpUserName;
    }

    @JsonProperty("SftpPassword")
    public String getSftpPasswordEncrypted() {
        return sftpPasswordEncrypted;
    }

    @JsonProperty("SftpPassword")
    public void setSftpPasswordEncrypted(String sftpPasswordEncrypted) {
        this.sftpPasswordEncrypted = sftpPasswordEncrypted;
    }

    @JsonProperty("SftpDir")
    public String getSftpDir() {
        return sftpDir;
    }

    @JsonProperty("SftpDir")
    public void setSftpDir(String sftpDir) {
        this.sftpDir = sftpDir;
    }

    @JsonProperty("CheckVersion")
    public Integer getCheckVersion() {
        return checkVersion;
    }

    @JsonProperty("CheckVersion")
    public void setCheckVersion(Integer checkVersion) {
        this.checkVersion = checkVersion;
    }

    @JsonProperty("CheckStrategy")
    public FileCheckStrategy getCheckStrategy() {
        return checkStrategy;
    }

    @JsonProperty("CheckStrategy")
    public void setCheckStrategy(FileCheckStrategy checkStrategy) {
        this.checkStrategy = checkStrategy;
    }

    @JsonProperty("FileExtension")
    public String getFileExtension() {
        return fileExtension;
    }

    @JsonProperty("FileExtension")
    public void setFileExtension(String fileExtension) {
        this.fileExtension = fileExtension;
    }

    @JsonProperty("FileNamePrefix")
    public String getFileNamePrefix() {
        return fileNamePrefix;
    }

    @JsonProperty("FileNamePrefix")
    public void setFileNamePrefix(String fileNamePrefix) {
        this.fileNamePrefix = fileNamePrefix;
    }

    @JsonProperty("FileNamePostfix")
    public String getFileNamePostfix() {
        return fileNamePostfix;
    }

    @JsonProperty("FileNamePostfix")
    public void setFileNamePostfix(String fileNamePostfix) {
        this.fileNamePostfix = fileNamePostfix;
    }

    @JsonProperty("FileTimestamp")
    public String getFileTimestamp() {
        return fileTimestamp;
    }

    @JsonProperty("FileTimestamp")
    public void setFileTimestamp(String fileTimestamp) {
        this.fileTimestamp = fileTimestamp;
    }

}
