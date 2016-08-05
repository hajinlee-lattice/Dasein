package com.latticeengines.domain.exposed.modelquality;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.CipherUtils;

public class Environment {

    @JsonProperty("tenant")
    public String tenant;
    
    @JsonProperty("username")
    public String username;
    
    @JsonProperty("encrypted_password")
    public String encryptedPassword;
    
    @JsonProperty("api_host_port")
    public String apiHostPort;
    
    private String password = null;
    
    public String getPassword() {
        if (password == null) {
            password = CipherUtils.decrypt(encryptedPassword);
        }
        return password;
    }
    
}
