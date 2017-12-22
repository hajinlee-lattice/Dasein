package com.latticeengines.domain.exposed.saml;

public class LoginValidationResponse {

    private boolean isValidated;

    private String userId;

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

}
