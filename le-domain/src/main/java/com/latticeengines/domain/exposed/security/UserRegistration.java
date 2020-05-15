package com.latticeengines.domain.exposed.security;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class UserRegistration {

    private User user;
    private Credentials credentials;
    private boolean validation = false;
    private boolean isDCP = false;

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

    @JsonProperty("IsDCP")
    public boolean isDCP() {
        return isDCP;
    }

    @JsonProperty("IsDCP")
    public void setDCP(boolean DCP) {
        isDCP = DCP;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public void toLowerCase() {
        this.user.setUsername(this.user.getUsername().toLowerCase());
        this.user.setEmail(this.user.getEmail().toLowerCase());
        this.credentials.setUsername(this.credentials.getUsername().toLowerCase());
    }

}
