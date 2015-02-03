package com.latticeengines.domain.exposed.security;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Credentials {

    private String username;
    private String password;
    
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
