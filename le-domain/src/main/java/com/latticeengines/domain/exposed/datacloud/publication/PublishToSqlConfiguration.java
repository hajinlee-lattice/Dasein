package com.latticeengines.domain.exposed.datacloud.publication;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PublishToSqlConfiguration extends PublicationConfiguration {

    protected String host;
    protected Integer port;
    protected String dbType = "SQLServer";
    protected String database;
    protected String defaultTableName;
    protected String username;
    protected String encryptedPassword;
    protected Alias alias;

    @Override
    @JsonProperty("ConfigurationType")
    public String getConfigurationType() {
        return this.getClass().getSimpleName();
    }

    @JsonProperty("Alias")
    public Alias getAlias() {
        return alias;
    }

    @JsonProperty("Alias")
    public void setAlias(Alias alias) {
        this.alias = alias;
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

    @JsonProperty("DbType")
    public String getDbType() {
        return dbType;
    }

    @JsonProperty("DbType")
    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    @JsonProperty("DefaultTableName")
    public String getDefaultTableName() {
        return defaultTableName;
    }

    @JsonProperty("DefaultTableName")
    public void setDefaultTableName(String defaultTableName) {
        this.defaultTableName = defaultTableName;
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

    public enum Alias {
        CollectionDB, BulkDB, SourceDB, TestDB
    }
}
