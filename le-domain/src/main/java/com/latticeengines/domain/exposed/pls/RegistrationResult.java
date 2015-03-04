package com.latticeengines.domain.exposed.pls;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.security.User;

public class RegistrationResult {
    private String password;
    private User user;
    private boolean valid;

    @JsonProperty("Password")
    public String getPassword() { return password; }

    @JsonProperty("Password")
    public void setPassword(String password) { this.password = password; }

    @JsonProperty("User")
    public User getUser() { return user; }

    @JsonProperty("User")
    public void setUser(User user) { this.user = user; }

    @JsonProperty("Valid")
    public boolean isValid() { return valid; }

    @JsonProperty("Valid")
    public void setValid(boolean valid) { this.valid = valid; }
}
