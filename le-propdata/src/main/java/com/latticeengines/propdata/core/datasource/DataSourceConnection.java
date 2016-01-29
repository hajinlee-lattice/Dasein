package com.latticeengines.propdata.core.datasource;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DataSourceConnection {

    private String driver;
    private String url;
    private String username;
    private String password;

    @JsonProperty("Driver")
    public String getDriver() {
        return driver;
    }

    @JsonProperty("Driver")
    public void setDriver(String driver) {
        this.driver = driver;
    }

    @JsonProperty("Url")
    public String getUrl() {
        return url;
    }

    @JsonProperty("Url")
    public void setUrl(String url) {
        this.url = url;
    }

    @JsonProperty("Username")
    public String getUsername() {
        return username;
    }

    @JsonProperty("Username")
    public void setUsername(String username) {
        this.username = username;
    }

    @JsonProperty("Password")
    public String getPassword() {
        return password;
    }

    @JsonProperty("Password")
    public void setPassword(String password) {
        this.password = password;
    }
}
