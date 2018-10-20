package com.latticeengines.domain.exposed.saml;

import java.util.List;

public class LoginValidationResponse {

    private boolean isValidated;

    private String userId;

    private String firstName;

    private String lastName;

    private List<String> userRoles;

    private Exception authenticationException;

    public boolean isValidated() {
        return isValidated;
    }

    public void setValidated(boolean isValidated) {
        this.isValidated = isValidated;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Exception getAuthenticationException() {
        return authenticationException;
    }

    public void setAuthenticationException(Exception authenticationException) {
        this.authenticationException = authenticationException;
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

    public List<String> getUserRoles() {
        return userRoles;
    }

    public void setUserRoles(List<String> userRoles) {
        this.userRoles = userRoles;
    }

}
