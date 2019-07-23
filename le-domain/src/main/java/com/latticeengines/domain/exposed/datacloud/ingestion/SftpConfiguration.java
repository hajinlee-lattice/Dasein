package com.latticeengines.domain.exposed.datacloud.ingestion;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SftpConfiguration extends ProviderConfiguration {

    @JsonProperty("SftpHost")
    private String sftpHost;

    @JsonProperty("SftpPort")
    private Integer sftpPort;

    @JsonProperty("SftpUsername")
    private String sftpUserName;

    @JsonProperty("SftpPassword")
    private String sftpPasswordEncrypted;

    @JsonProperty("SftpDir")
    private String sftpDir;

    @JsonProperty("CheckStrategy")
    private VersionCheckStrategy checkStrategy;

    @JsonProperty("FileExtension")
    private String fileExtension;

    @JsonProperty("FileNamePrefix")
    private String fileNamePrefix;

    @JsonProperty("FileNamePostfix")
    private String fileNamePostfix;

    @JsonProperty("FileTimestamp")
    private String fileTimestamp;

    // whether different versions of files are separate sub-folders under
    // sftpDir
    @JsonProperty("HasSubFolder")
    private boolean hasSubFolder;

    // timestamp pattern in sub-folder name
    @JsonProperty("SubFolderTSPattern")
    private String subFolderTSPattern;

    public String getSftpHost() {
        return sftpHost;
    }

    public void setSftpHost(String sftpHost) {
        this.sftpHost = sftpHost;
    }

    public Integer getSftpPort() {
        return sftpPort;
    }

    public void setSftpPort(Integer sftpPort) {
        this.sftpPort = sftpPort;
    }

    public String getSftpUserName() {
        return sftpUserName;
    }

    public void setSftpUserName(String sftpUserName) {
        this.sftpUserName = sftpUserName;
    }

    public String getSftpPasswordEncrypted() {
        return sftpPasswordEncrypted;
    }

    public void setSftpPasswordEncrypted(String sftpPasswordEncrypted) {
        this.sftpPasswordEncrypted = sftpPasswordEncrypted;
    }

    public String getSftpDir() {
        return sftpDir == null ? "" : sftpDir;
    }

    public void setSftpDir(String sftpDir) {
        this.sftpDir = sftpDir;
    }

    public VersionCheckStrategy getCheckStrategy() {
        return checkStrategy;
    }

    public void setCheckStrategy(VersionCheckStrategy checkStrategy) {
        this.checkStrategy = checkStrategy;
    }

    public String getFileExtension() {
        return fileExtension;
    }

    public void setFileExtension(String fileExtension) {
        this.fileExtension = fileExtension;
    }

    public String getFileNamePrefix() {
        return fileNamePrefix;
    }

    public void setFileNamePrefix(String fileNamePrefix) {
        this.fileNamePrefix = fileNamePrefix;
    }

    public String getFileNamePostfix() {
        return fileNamePostfix;
    }

    public void setFileNamePostfix(String fileNamePostfix) {
        this.fileNamePostfix = fileNamePostfix;
    }

    public String getFileTimestamp() {
        return fileTimestamp;
    }

    public void setFileTimestamp(String fileTimestamp) {
        this.fileTimestamp = fileTimestamp;
    }

    public boolean hasSubFolder() {
        return hasSubFolder;
    }

    public void setHasSubFolder(boolean hasSubFolder) {
        this.hasSubFolder = hasSubFolder;
    }

    public String getSubFolderTSPattern() {
        return subFolderTSPattern;
    }

    public void setSubFolderTSPattern(String subFolderTSPattern) {
        this.subFolderTSPattern = subFolderTSPattern;
    }
}
