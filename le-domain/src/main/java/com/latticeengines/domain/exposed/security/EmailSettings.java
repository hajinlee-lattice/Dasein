package com.latticeengines.domain.exposed.security;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class EmailSettings {

    private String from;
    private String password;
    private int port;
    private String server;
    private boolean useSTARTTLS;
    private boolean useSSL;
    private String username;

    @JsonProperty("From")
    public String getFrom() { return from; }

    @JsonProperty("From")
    public void setFrom(String from) {
        this.from = from;
    }

    @JsonProperty("Password")
    public String getPassword() { return password; }

    @JsonProperty("Password")
    public void setPassword(String password) {
        this.password = password;
    }

    @JsonProperty("Port")
    public int getPort() { return port; }

    @JsonProperty("Port")
    public void setPort(int port) {
        this.port = port;
    }

    @JsonProperty("Server")
    public String getServer() {
        return server;
    }

    @JsonProperty("Server")
    public void setServer(String server) {
        this.server = server;
    }

    @JsonProperty("UseSSL")
    public boolean isUseSSL() { return useSSL; }

    @JsonProperty("UseSSL")
    public void setUseSSL(boolean useSSL) {
        this.useSSL = useSSL;
    }

    @JsonProperty("UseSTARTTLS")
    public boolean isUseSTARTTLS() { return useSTARTTLS; }

    @JsonProperty("UseSTARTTLS")
    public void setUseSTARTTLS(boolean useSTARTTLS) { this.useSTARTTLS = useSTARTTLS; }

    @JsonProperty("Username")
    public String getUsername() {
        return username;
    }

    @JsonProperty("Username")
    public void setUsername(String username) {
        this.username = username;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
