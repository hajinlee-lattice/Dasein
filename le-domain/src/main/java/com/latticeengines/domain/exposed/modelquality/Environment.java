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
    
    public static Environment getCurrentEnvironment()
    {
        // hardcoded for now, fix it by getting the info off of config files 
        Environment env = new Environment();
        env.tenant = "ModelQualityExperiments";
        env.username = "bnguyen@lattice-engines.com";
        env.password = "tahoe";
        env.apiHostPort = "https://bodcdevtca18.lattice.local:8081";
        return env;
    }
}
