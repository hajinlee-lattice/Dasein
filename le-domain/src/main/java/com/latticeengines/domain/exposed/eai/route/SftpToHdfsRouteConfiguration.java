package com.latticeengines.domain.exposed.eai.route;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SftpToHdfsRouteConfiguration extends CamelRouteConfiguration {

    public static final String OPEN_SUFFIX = "_DOWNLOADING_";

    private String sftpHost;
    private Integer sftpPort;
    private String sftpUserName;
    private String sftpPasswordEncrypted;
    private String sftpDir;
    private String fileName;
    private String hdfsDir;

    @JsonProperty("sftp_host")
    public String getSftpHost() {
        return sftpHost;
    }

    @JsonProperty("sftp_host")
    public void setSftpHost(String sftpHost) {
        this.sftpHost = sftpHost;
    }

    @JsonProperty("sftp_port")
    public Integer getSftpPort() {
        return sftpPort;
    }

    @JsonProperty("sftp_port")
    public void setSftpPort(Integer sftpPort) {
        this.sftpPort = sftpPort;
    }

    @JsonProperty("sftp_username")
    public String getSftpUserName() {
        return sftpUserName;
    }

    @JsonProperty("sftp_username")
    public void setSftpUserName(String sftpUserName) {
        this.sftpUserName = sftpUserName;
    }

    @JsonProperty("sftp_password")
    public String getSftpPasswordEncrypted() {
        return sftpPasswordEncrypted;
    }

    @JsonProperty("sftp_password")
    public void setSftpPasswordEncrypted(String sftpPasswordEncrypted) {
        this.sftpPasswordEncrypted = sftpPasswordEncrypted;
    }

    @JsonProperty("sftp_dir")
    public String getSftpDir() {
        return sftpDir;
    }

    @JsonProperty("sftp_dir")
    public void setSftpDir(String sftpDir) {
        this.sftpDir = sftpDir;
    }

    @JsonProperty("file_name")
    public String getFileName() {
        return fileName;
    }

    @JsonProperty("file_name")
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    @JsonProperty("hdfs_dir")
    public String getHdfsDir() {
        return hdfsDir;
    }

    @JsonProperty("hdfs_dir")
    public void setHdfsDir(String hdfsDir) {
        this.hdfsDir = hdfsDir;
    }
}
