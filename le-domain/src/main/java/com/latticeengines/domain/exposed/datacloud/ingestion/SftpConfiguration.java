package com.latticeengines.domain.exposed.datacloud.ingestion;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Keep doc
 * https://confluence.lattice-engines.com/display/ENG/DataCloud+Engine+Architecture#DataCloudEngineArchitecture-Ingestion
 * up to date if there is any new change
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SftpConfiguration extends ProviderConfiguration {

    // SFTP url
    @JsonProperty("SftpHost")
    private String sftpHost;

    // SFTP port
    @JsonProperty("SftpPort")
    private Integer sftpPort;

    // SFTP username
    @JsonProperty("SftpUsername")
    private String sftpUserName;

    // SFTP password encrypted by {@link CipherUtils}
    @JsonProperty("SftpPassword")
    private String sftpPasswordEncrypted;

    // SFTP root folder path
    @JsonProperty("SftpDir")
    private String sftpDir;

    // When backtracking versions to check missing files, the number of versions
    // is defined by day, week, month, or all(checking all the versions
    // with {@link #checkVersion} ignored)
    @JsonProperty("CheckStrategy")
    private VersionCheckStrategy checkStrategy;

    // Whether there are sub-folders under root folder, eg. different versions
    // of files are in different sub-folders
    @JsonProperty("HasSubfolder")
    private boolean hasSubfolder;

    // Regex pattern of subfolder name, eg. \\d{4}-\\d{2}-\\d{2}
    // Only effective when {@link #hasSubfolder} is true
    // If not provided when {@link #hasSubfolder} is true, default value is (.+)
    // to match all the subfolders
    @JsonProperty("SubfolderRegexPattern")
    private String subfolderRegexPattern;

    // If subfolder is versioned with timestamp (currently only timestamp format
    // version is supported), provider pattern for timestamp part, eg. yyyyMMdd
    @JsonProperty("SubfolderTSPattern")
    private String subfolderTSPattern;

    // Regex pattern of file name, eg.
    // AllDomainsAllTopicsZips_\\d{4}-\\d{2}-\\d{2}_\\d+.csv.gz
    // If not provided, default value is (.+) to match all the files
    @JsonProperty("FileRegexPattern")
    private String fileRegexPattern;

    // If file name is versioned with timestamp (currently only timestamp format
    // version is supported), provider pattern for timestamp part, eg. yyyyMMdd
    // Timezone info will be ignored and use local timezone
    @JsonProperty("FileTSPattern")
    private String fileTSPattern;

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

    public boolean hasSubfolder() {
        return hasSubfolder;
    }

    public void setHasSubfolder(boolean hasSubfolder) {
        this.hasSubfolder = hasSubfolder;
    }

    public String getSubfolderRegexPattern() {
        return subfolderRegexPattern == null ? "(.+)" : subfolderRegexPattern;
    }

    public void setSubfolderRegexPattern(String subfolderRegexPattern) {
        this.subfolderRegexPattern = subfolderRegexPattern;
    }

    public String getSubfolderTSPattern() {
        return subfolderTSPattern;
    }

    public void setSubfolderTSPattern(String subfolderTSPattern) {
        this.subfolderTSPattern = subfolderTSPattern;
    }

    public String getFileRegexPattern() {
        return fileRegexPattern == null ? "(.+)" : fileRegexPattern;
    }

    public void setFileRegexPattern(String fileRegexPattern) {
        this.fileRegexPattern = fileRegexPattern;
    }

    public String getFileTSPattern() {
        return fileTSPattern;
    }

    public void setFileTSPattern(String fileTSPattern) {
        this.fileTSPattern = fileTSPattern;
    }

}
