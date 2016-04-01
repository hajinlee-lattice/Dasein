package com.latticeengines.domain.exposed.modeling;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.CipherUtils;

public class DbCreds {

    private static Log log = LogFactory.getLog(DbCreds.class);

    private String user;
    private String password;
    private String encryptedPassword;
    private String host;
    private int port;
    private String db;
    private String dbType;
    private String instance;
    private String jdbcUrl;
    private String driverClass;
    
    public DbCreds() {
    }

    public DbCreds(Builder builder) {
        this.user = builder.user;
        this.password = builder.password;
        this.encryptedPassword = builder.encryptedPassword;
        this.host = builder.host;
        this.port = builder.port;
        this.db = builder.db;
        this.dbType = builder.dbType;
        this.instance = builder.instance;
        this.jdbcUrl = builder.jdbcUrl;
        this.driverClass = builder.driverClass;
    }

    @PostConstruct
    private void postConstruct() {
        if (StringUtils.isNotEmpty(this.encryptedPassword)) {
            log.info("Overwrite clear text password by the encrypted one.");
            this.password = CipherUtils.decrypt(encryptedPassword);
        }
    }

    @JsonProperty("user")
    public String getUser() {
        return user;
    }

    @JsonProperty("user")
    public void setUser(String user) {
        this.user = user;
    }

    @JsonProperty("password")
    public String getPassword() {
        return password;
    }

    @JsonProperty("password")
    public void setPassword(String password) {
        this.password = password;
    }

    @JsonProperty("encrypted_password")
    private String getEncryptedPassword() {
        return encryptedPassword;
    }

    @JsonProperty("encrypted_password")
    private void setEncryptedPassword(String encryptedPassword) {
        this.encryptedPassword = encryptedPassword;
    }

    @JsonProperty("host")
    public String getHost() {
        return host;
    }

    @JsonProperty("host")
    public void setHost(String host) {
        this.host = host;
    }

    @JsonProperty("port")
    public int getPort() {
        return port;
    }

    @JsonProperty("port")
    public void setPort(int port) {
        this.port = port;
    }

    @JsonProperty("db")
    public String getDb() {
        return db;
    }

    @JsonProperty("db")
    public void setDb(String db) {
        this.db = db;
    }

    @JsonProperty("db_type")
    public String getDbType() {
        return dbType;
    }

    @JsonProperty("db_type")
    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    @JsonProperty(value = "instance", required = false)
    public String getInstance() {
        return instance;
    }

    @JsonProperty("instance")
    public void setInstance(String instance) {
        this.instance = instance;
    }

    @JsonProperty(value = "jdbc_url", required = false)
    public String getJdbcUrl() {
        return jdbcUrl;
    }

    @JsonProperty("jdbc_url")
    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    @JsonProperty(value = "driver_class", required = false)
    public String getDriverClass() {
        return driverClass;
    }

    @JsonProperty("driver_class")
    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    public static class Builder {

        private String user;
        private String password;
        private String encryptedPassword;
        private String host;
        private int port;
        private String db;
        private String dbType = "SQLServer";
        private String instance;
        private String jdbcUrl;
        private String driverClass;

        public Builder() {
        }

        public Builder user(String user) {
            this.user = user;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            this.encryptedPassword = CipherUtils.encrypt(password);
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
        
        public Builder instance(String instance) {
            this.instance = instance;
            return this;
        }
        
        public Builder jdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }
        
        public Builder driverClass(String driverClass) {
            this.driverClass = driverClass;
            return this;
        }
        
    }

}