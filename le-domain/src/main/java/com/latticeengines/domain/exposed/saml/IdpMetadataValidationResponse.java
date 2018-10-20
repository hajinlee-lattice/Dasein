package com.latticeengines.domain.exposed.saml;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class IdpMetadataValidationResponse {

    private boolean isValid;

    private String entityId;

    private String exceptionMessage;

    private String singleSignOnService;

    public boolean isValid() {
        return isValid;
    }

    public void setValid(boolean isValid) {
        this.isValid = isValid;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public String getExceptionMessage() {
        return exceptionMessage;
    }

    public void setExceptionMessage(String exceptionMessage) {
        this.exceptionMessage = exceptionMessage;
    }

    public String getSingleSignOnService() {
        return singleSignOnService;
    }

    public void setSingleSignOnService(String singleSignOnService) {
        this.singleSignOnService = singleSignOnService;
    }
}
