package com.latticeengines.domain.exposed.modeling;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DbCreds {

    private String user;
    private String password;
    private String host;
    private int port;
    private String db;
    private String dbType;
    
    public DbCreds() {
    }

    public DbCreds(Builder builder) {
        this.user = builder.user;
        this.password = builder.password;
        this.host = builder.host;
        this.port = builder.port;
        this.db = builder.db;
        this.setDBType(builder.dbType);
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    @JsonProperty("db_type")
    public String getDBType() {
        return dbType;
    }

    public void setDBType(String dbType) {
        this.dbType = dbType;
    }

    public static class Builder {

        private String user;
        private String password;
        private String host;
        private int port;
        private String db;
        private String dbType = "SQLServer";

        public Builder() {
        }

        public Builder user(String user) {
            this.user = user;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder db(String db) {
            this.db = db;
            return this;
        }

        public Builder dbType(String dbType) {
            this.dbType = dbType;
            return this;
        }
}

}