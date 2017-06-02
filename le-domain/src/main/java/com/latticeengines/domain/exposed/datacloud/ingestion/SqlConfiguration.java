package com.latticeengines.domain.exposed.datacloud.ingestion;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SqlConfiguration extends ProviderConfiguration {
    private String dbHost;
    private Integer dbPort;
    private String db;
    private String dbUser;
    private String dbPwdEncrypted;
    private String dbTable;

    @JsonProperty("DbHost")
    public String getDbHost() {
        return dbHost;
    }

    @JsonProperty("DbHost")
    public void setDbHost(String dbHost) {
        this.dbHost = dbHost;
    }

    @JsonProperty("DbPort")
    public Integer getDbPort() {
        return dbPort;
    }

    @JsonProperty("DbPort")
    public void setDbPort(Integer dbPort) {
        this.dbPort = dbPort;
    }

    @JsonProperty("Db")
    public String getDb() {
        return db;
    }

    @JsonProperty("Db")
    public void setDb(String db) {
        this.db = db;
    }

    @JsonProperty("DbUser")
    public String getDbUser() {
        return dbUser;
    }

    @JsonProperty("DbUser")
    public void setDbUser(String dbUser) {
        this.dbUser = dbUser;
    }

    @JsonProperty("DbPwdEncrypted")
    public String getDbPwdEncrypted() {
        return dbPwdEncrypted;
    }

    @JsonProperty("DbPwdEncrypted")
    public void setDbPwdEncrypted(String dbPwdEncrypted) {
        this.dbPwdEncrypted = dbPwdEncrypted;
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
