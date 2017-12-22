package com.latticeengines.domain.exposed.saml;

public class LogoutValidationResponse {

    private boolean isValidated;

    public boolean isValidated() {
        return isValidated;
    }

    public void setValidated(boolean isValidated) {
        this.isValidated = isValidated;
    }

}
