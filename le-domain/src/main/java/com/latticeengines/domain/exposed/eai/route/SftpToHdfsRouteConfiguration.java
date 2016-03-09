package com.latticeengines.domain.exposed.eai.route;

public class SftpToHdfsRouteConfiguration extends CamelRouteConfiguration {

    public static final String OPEN_SUFFIX = "_DOWNLOADING_";

    private String sftpHost;
    private Integer sftpPort;
    private String sftpUserName;
    private String sftpPasswordEncrypted;
    private String sftpDir;
    private String fileName;
    private String hdfsDir;

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
        return sftpDir;
    }

    public void setSftpDir(String sftpDir) {
        this.sftpDir = sftpDir;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getHdfsDir() {
        return hdfsDir;
    }

    public void setHdfsDir(String hdfsDir) {
        this.hdfsDir = hdfsDir;
    }
}
