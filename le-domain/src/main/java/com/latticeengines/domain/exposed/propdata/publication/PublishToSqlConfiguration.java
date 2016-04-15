package com.latticeengines.domain.exposed.propdata.publication;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PublishToSqlConfiguration extends PublicationConfiguration {

    private String host;
    private Integer port;
    private String database;
    private String defaultTableName;
    private String username;
    private String encryptedPassword;
    private PublicationStrategy publicationStrategy;

    @Override
    @JsonProperty("ConfigurationType")
    public String getConfigurationType() {
        return this.getClass().getSimpleName();
    }

    @JsonProperty("Host")
    public String getHost() {
        return host;
    }

    @JsonProperty("Host")
    public void setHost(String host) {
        this.host = host;
    }

    @JsonProperty("Port")
    public Integer getPort() {
        return port;
    }

    @JsonProperty("Port")
    public void setPort(Integer port) {
        this.port = port;
    }

    @JsonProperty("Database")
    public String getDatabase() {
        return database;
    }

    @JsonProperty("Database")
    public void setDatabase(String database) {
        this.database = database;
    }

    @JsonProperty("DefaultTableName")
    public String getDefaultTableName() {
        return defaultTableName;
    }

    @JsonProperty("DefaultTableName")
    public void setDefaultTableName(String defaultTableName) {
        this.defaultTableName = defaultTableName;
    }

    @JsonProperty("Strategy")
    public PublicationStrategy getPublicationStrategy() {
        return publicationStrategy;
    }

    @JsonProperty("Strategy")
    public void setPublicationStrategy(PublicationStrategy publicationStrategy) {
        this.publicationStrategy = publicationStrategy;
    }

    @JsonProperty("Username")
    public String getUsername() {
        return username;
    }

    @JsonProperty("Username")
    public void setUsername(String username) {
        this.username = username;
    }

    @JsonProperty("EncryptedPassword")
    public String getEncryptedPassword() {
        return encryptedPassword;
    }

    @JsonProperty("EncryptedPassword")
    public void setEncryptedPassword(String encryptedPassword) {
        this.encryptedPassword = encryptedPassword;
    }

    public enum PublicationStrategy {
        VERSIONED, REPLACE, APPEND
    }

}
