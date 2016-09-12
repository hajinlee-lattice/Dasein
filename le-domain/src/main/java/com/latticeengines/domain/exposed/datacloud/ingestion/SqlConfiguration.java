package com.latticeengines.domain.exposed.datacloud.ingestion;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SqlConfiguration extends ProviderConfiguration {
    private String dbUrl;
    private String dbUserName;
    private String dbPasswordEncrypted;
    private String dbDriver;
    private String dbTable;

    @JsonProperty("DbUrl")
    public String getDbUrl() {
        return dbUrl;
    }

    @JsonProperty("DbUrl")
    public void setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    @JsonProperty("DbUserName")
    public String getDbUserName() {
        return dbUserName;
    }

    @JsonProperty("DbUserName")
    public void setDbUserName(String dbUserName) {
        this.dbUserName = dbUserName;
    }

    @JsonProperty("DbPasswordEncrypted")
    public String getDbPasswordEncrypted() {
        return dbPasswordEncrypted;
    }

    @JsonProperty("DbPasswordEncrypted")
    public void setDbPasswordEncrypted(String dbPasswordEncrypted) {
        this.dbPasswordEncrypted = dbPasswordEncrypted;
    }

    @JsonProperty("DbDriver")
    public String getDbDriver() {
        return dbDriver;
    }

    @JsonProperty("DbDriver")
    public void setDbDriver(String dbDriver) {
        this.dbDriver = dbDriver;
    }

    @JsonProperty("DbTable")
    public String getDbTable() {
        return dbTable;
    }

    @JsonProperty("DbTable")
    public void setDbTable(String dbTable) {
        this.dbTable = dbTable;
    }

}
