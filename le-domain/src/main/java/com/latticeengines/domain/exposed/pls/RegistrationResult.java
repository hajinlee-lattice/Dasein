package com.latticeengines.domain.exposed.pls;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.security.User;

public class RegistrationResult {
    private String password;
    private User conflictingUser;
    private boolean valid;

    @JsonProperty("Password")
    public String getPassword() { return password; }

    @JsonProperty("Password")
    public void setPassword(String password) { this.password = password; }

    @JsonProperty("ConflictingUser")
    public User getConflictingUser() { return conflictingUser; }

    @JsonProperty("ConflictingUser")
    public void setConflictingUser(User conflictingUser) { this.conflictingUser = conflictingUser; }

    @JsonProperty("Valid")
    public boolean isValid() { return valid; }

    @JsonProperty("Valid")
    public void setValid(boolean valid) { this.valid = valid; }

    @Override
    public String toString() { return JsonUtils.serialize(this); }
}
