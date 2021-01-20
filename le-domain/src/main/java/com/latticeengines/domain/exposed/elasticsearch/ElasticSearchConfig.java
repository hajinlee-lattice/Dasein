package com.latticeengines.domain.exposed.elasticsearch;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ElasticSearchConfig implements Serializable {

    private static final long serialVersionUID = 0L;

    @JsonProperty("Shards")
    private Integer shards;
    @JsonProperty("Replicas")
    private Integer replicas;
    @JsonProperty("RefreshInterval")
    private String refreshInterval;
    @JsonProperty("Dynamic")
    private Boolean dynamic;
    @JsonProperty("HttpScheme")
    private String httpScheme;
    @JsonProperty("ESHost")
    private String esHost;
    @JsonProperty("ESPort")
    private String esPort;
    @JsonProperty("EsUser")
    private String esUser;
    @JsonProperty("EsPassword")
    private String esPassword;

    @JsonProperty("EncryptionKey")
    private String encryptionKey;

    @JsonProperty("Salt")
    private String salt;

    public Integer getShards() {
        return shards;
    }

    public void setShards(Integer shards) {
        this.shards = shards;
    }

    public Integer getReplicas() {
        return replicas;
    }

    public void setReplicas(Integer replicas) {
        this.replicas = replicas;
    }

    public String getRefreshInterval() {
        return refreshInterval;
    }

    public void setRefreshInterval(String refreshInterval) {
        this.refreshInterval = refreshInterval;
    }

    public Boolean getDynamic() {
        return dynamic;
    }

    public void setDynamic(Boolean dynamic) {
        this.dynamic = dynamic;
    }

    public String getEsHost() {
        return esHost;
    }

    public void setEsHost(String esHost) {
        this.esHost = esHost;
    }

    public String getEsPort() {
        return esPort;
    }

    public void setEsPort(String esPort) {
        this.esPort = esPort;
    }

    public String getEsUser() {
        return esUser;
    }

    public void setEsUser(String esUser) {
        this.esUser = esUser;
    }

    public String getEsPassword() {
        return esPassword;
    }

    public void setEsPassword(String esPassword) {
        this.esPassword = esPassword;
    }

    public String getHttpScheme() {
        return httpScheme;
    }

    public void setHttpScheme(String httpScheme) {
        this.httpScheme = httpScheme;
    }

    public String getEncryptionKey() {
        return encryptionKey;
    }

    public void setEncryptionKey(String encryptionKey) {
        this.encryptionKey = encryptionKey;
    }

    public String getSalt() {
        return salt;
    }

    public void setSalt(String salt) {
        this.salt = salt;
    }
}
