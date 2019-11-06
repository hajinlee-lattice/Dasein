package com.latticeengines.domain.exposed.datacloud.ingestion;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

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

    // When backtracking versions to check missing files, the number of versions
    // is defined by day, week or month, or all -- checking all the versions
    // with {@link #checkVersion} ignored
    @JsonProperty("CheckStrategy")
    private VersionCheckStrategy checkStrategy;

    // When checking missing files, only look at files with specific extension;
    // Must provide; Currently only allow single kind of extension
    @JsonProperty("FileExtension")
    private String fileExtension;

    // Filename prefix is filename part before timestamp section; When checking
    // missing files, only look at files with prefix satisfying specific regex;
    // If no such restriction, fine to not provide
    @JsonProperty("FileNamePrefix")
    private String fileNamePrefix;

    // Filename prefix is filename part (excluding extension) after timestamp
    // section; When checking missing files, only look at files with postfix
    // satisfying specific regex; If no such restriction, fine to not provide
    @JsonProperty("FileNamePostfix")
    private String fileNamePostfix;

    // Only effective when {@link #checkStrategy} is not ALL; If {@link
    // #checkStrategy} is not ALL, version info must be provided in either file
    // name or subfolder name;
    // If file version is defined in file name, provide timestamp format string,
    // eg. yyyy-MM-dd
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
        Preconditions.checkNotNull(fileExtension);
        if (!fileExtension.startsWith(".")) {
            fileExtension = "." + fileExtension;
        }
        return fileExtension;
    }

    public void setFileExtension(String fileExtension) {
        this.fileExtension = fileExtension;
    }

    public String getFileNamePrefix() {
        return fileNamePrefix == null ? "(.*)" : fileNamePrefix;
    }

    public void setFileNamePrefix(String fileNamePrefix) {
        this.fileNamePrefix = fileNamePrefix;
    }

    public String getFileNamePostfix() {
        return fileNamePostfix == null ? "(.*)" : fileNamePostfix;
    }

    public void setFileNamePostfix(String fileNamePostfix) {
        this.fileNamePostfix = fileNamePostfix;
    }

    public String getFileTimestamp() {
        return fileTimestamp == null ? "" : fileTimestamp;
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
