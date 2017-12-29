package com.latticeengines.domain.exposed.pls;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.security.Tenant;

public class LoginDocument {

    private List<String> errors;
    private LoginResult result;
    private String uniqueness;
    private String randomness;
    private boolean success;
    private String authenticationRoute;

    public LoginDocument() {

    }

    public LoginDocument(String ticketData) {
        String[] tokens = ticketData.split("\\.");
        setUniqueness(tokens[0]);
        setRandomness(tokens[1]);
    }

    @JsonProperty("Errors")
    public List<String> getErrors() {
        return errors;
    }

    @JsonProperty("Errors")
    public void setErrors(List<String> errors) {
        this.errors = errors;
    }

    @JsonProperty("Uniqueness")
    public String getUniqueness() {
        return uniqueness;
    }

    @JsonProperty("Uniqueness")
    public void setUniqueness(String uniqueness) {
        this.uniqueness = uniqueness;
    }

    @JsonProperty("Randomness")
    public String getRandomness() {
        return randomness;
    }

    @JsonProperty("Randomness")
    public void setRandomness(String randomness) {
        this.randomness = randomness;
    }

    @JsonProperty("Result")
    public LoginResult getResult() {
        return result;
    }

    @JsonProperty("Result")
    public void setResult(LoginResult result) {
        this.result = result;
    }

    @JsonProperty("Success")
    public boolean isSuccess() {
        return success;
    }

    @JsonProperty("Success")
    public void setSuccess(boolean success) {
        this.success = success;
    }

    @JsonProperty("AuthenticationRoute")
    public String getAuthenticationRoute() {
        return authenticationRoute;
    }

    @JsonProperty("AuthenticationRoute")
    public void setAuthenticationRoute(String authenticationRoute) {
        this.authenticationRoute = authenticationRoute;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @JsonIgnore
    public String getData() {
        return getUniqueness() + "." + getRandomness();
    }

    public class LoginResult {
        private List<Tenant> tenants;
        private boolean mustChangePassword;
        private long passwordLastModified;

        public LoginResult() {

        }

        @JsonProperty("Tenants")
        public List<Tenant> getTenants() {
            return tenants;
        }

        @JsonProperty("Tenants")
        public void setTenants(List<Tenant> tenants) {
            this.tenants = tenants;
        }

        @JsonProperty("MustChangePassword")
        public boolean isMustChangePassword() {
            return mustChangePassword;
        }

        @JsonProperty("MustChangePassword")
        public void setMustChangePassword(boolean mustChangePassword) {
            this.mustChangePassword = mustChangePassword;
        }

        @JsonProperty("PasswordLastModified")
        public long getPasswordLastModified() {
            return passwordLastModified;
        }

        @JsonProperty("PasswordLastModified")
        public void setPasswordLastModified(long passwordLastModified) {
            this.passwordLastModified = passwordLastModified;
        }

    }

}
