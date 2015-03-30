package com.latticeengines.domain.exposed.security;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class UserRegistration {

    private User user;
    private Credentials credentials;
    private boolean validation = false;
    private String accessLevel;
    
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

    @JsonProperty("Validation")
    public boolean getValidation() {
        return validation;
    }

    @JsonProperty("Validation")
    public void setValidation(boolean validation) {
        this.validation = validation;
    }

    @JsonProperty("AccessLevel")
    public String getAccessLevel() { return accessLevel; }

    @JsonProperty("AccessLevel")
    public void setAccessLevel(String accessLevel) { this.accessLevel = accessLevel; }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
    
}
