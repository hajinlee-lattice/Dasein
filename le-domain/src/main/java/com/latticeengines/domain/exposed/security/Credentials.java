package com.latticeengines.domain.exposed.security;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

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
    
    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
