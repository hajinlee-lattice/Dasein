package com.latticeengines.domain.exposed.propdata.ingestion;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SftpConfiguration extends ProviderConfiguration {

    private String sftpHost;
    private Integer sftpPort;
    private String sftpUserName;
    private String sftpPasswordEncrypted;
    private String sftpDir;

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
}
