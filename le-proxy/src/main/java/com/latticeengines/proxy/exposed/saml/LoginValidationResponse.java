package com.latticeengines.proxy.exposed.saml;

public class LoginValidationResponse {

    private boolean isValidated;

    private String userId;

    public boolean isValidated() {
        return isValidated;
    }

    public void setValidated(boolean isValidated) {
        this.isValidated = isValidated;
    }

    String getUserId() {
        return userId;
    }

    void setUserId(String userId) {
        this.userId = userId;
    }

}
