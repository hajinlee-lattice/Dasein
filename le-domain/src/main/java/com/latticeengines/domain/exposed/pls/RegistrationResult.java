package com.latticeengines.domain.exposed.pls;


import com.fasterxml.jackson.annotation.JsonProperty;

public class RegistrationResult {
    private String password;

    @JsonProperty("Password")
    public String getPassword() { return password; }

    @JsonProperty("Password")
    public void setPassword(String password) { this.password = password; }
}
