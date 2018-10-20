package com.latticeengines.domain.exposed.saml;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SamlConfigMetadata {

    private String entityId;

    private String singleSignOnService;

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    @JsonProperty("singleSignOnService")
    public String getSingleSignOnService() {
        return singleSignOnService;
    }

    public void setSingleSignOnService(String singleSignOnService) {
        this.singleSignOnService = singleSignOnService;
    }

}
