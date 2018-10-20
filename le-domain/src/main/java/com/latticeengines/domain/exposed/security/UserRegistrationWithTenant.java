package com.latticeengines.domain.exposed.security;

import com.fasterxml.jackson.annotation.JsonProperty;

public class UserRegistrationWithTenant {

    private UserRegistration userRegistration;
    private String tenant;

    @JsonProperty("UserRegistration")
    public UserRegistration getUserRegistration() {
        return userRegistration;
    }

    @JsonProperty("UserRegistration")
    public void setUserRegistration(UserRegistration userRegistration) {
        this.userRegistration = userRegistration;
    }

    @JsonProperty("Tenant")
    public String getTenant() {
        return tenant;
    }

    @JsonProperty("Tenant")
    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

}
