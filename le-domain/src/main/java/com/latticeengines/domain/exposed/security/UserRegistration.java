package com.latticeengines.domain.exposed.security;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class UserRegistration {

    private User user;
    private Credentials credentials;
    private Tenant tenant;
    
    @JsonProperty("User")
    public User getUser() {
        return user;
    }
    
    @JsonProperty("User")
    public void setUser(User user) {
        this.user = user;
    }

    @JsonProperty("Credentials")
    public Credentials getCredentials() {
        return credentials;
    }

    @JsonProperty("Credentials")
    public void setCredentials(Credentials credentials) {
        this.credentials = credentials;
    }

    @JsonProperty("Tenant")
    public Tenant getTenant() {
        return tenant;
    }

    @JsonProperty("Tenant")
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }
    
    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
    
}
