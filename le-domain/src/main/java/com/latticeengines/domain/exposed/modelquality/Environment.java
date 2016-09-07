package com.latticeengines.domain.exposed.modelquality;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Environment {

    @JsonProperty("tenant")
    public String tenant;
    
    @JsonProperty("username")
    public String username;
    
    @JsonProperty("password")
    public String password;
    
    @JsonProperty("api_host_port")
    public String apiHostPort;
    
    public String getPassword() {
        return password;
    }
    
}
