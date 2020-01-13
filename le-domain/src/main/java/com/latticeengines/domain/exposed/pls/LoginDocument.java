package com.latticeengines.domain.exposed.pls;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.security.Tenant;

public class LoginDocument {

    @JsonProperty("UserName")
    private String userName;

    @JsonProperty("FirstName")
    private String firstName;

    @JsonProperty("LastName")
    private String lastName;

    @JsonProperty("Uniqueness")
    private String uniqueness;

    @JsonProperty("Randomness")
    private String randomness;

    @JsonProperty("Errors")
    private List<String> errors;

    @JsonProperty("Success")
    private boolean success;

    @JsonProperty("AuthenticationRoute")
    private String authenticationRoute;

    @JsonProperty("Result")
    private LoginResult result;

    public LoginDocument() {

    }

    public LoginDocument(String ticketData) {
        String[] tokens = ticketData.split("\\.");
        setUniqueness(tokens[0]);
        setRandomness(tokens[1]);
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public List<String> getErrors() {
        return errors;
    }

    public void setErrors(List<String> errors) {
        this.errors = errors;
    }

    public String getUniqueness() {
        return uniqueness;
    }

    public void setUniqueness(String uniqueness) {
        this.uniqueness = uniqueness;
    }

    public String getRandomness() {
        return randomness;
    }

    public void setRandomness(String randomness) {
        this.randomness = randomness;
    }

    public LoginResult getResult() {
        return result;
    }

    public void setResult(LoginResult result) {
        this.result = result;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getAuthenticationRoute() {
        return authenticationRoute;
    }

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

        @JsonProperty("Tenants")
        private List<Tenant> tenants;

        @JsonProperty("MustChangePassword")
        private boolean mustChangePassword;

        @JsonProperty("PasswordLastModified")
        private long passwordLastModified;

        public LoginResult() {

        }

        public List<Tenant> getTenants() {
            return tenants;
        }

        public void setTenants(List<Tenant> tenants) {
            this.tenants = tenants;
        }

        public boolean isMustChangePassword() {
            return mustChangePassword;
        }

        public void setMustChangePassword(boolean mustChangePassword) {
            this.mustChangePassword = mustChangePassword;
        }

        public long getPasswordLastModified() {
            return passwordLastModified;
        }

        public void setPasswordLastModified(long passwordLastModified) {
            this.passwordLastModified = passwordLastModified;
        }

    }

}
