package com.latticeengines.domain.exposed.saml;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ServiceProviderURIInfo {

    @JsonProperty("assertionConsumerServiceURL")
    private String assertionConsumerServiceURL;

    @JsonProperty("serviceProviderMetadataURL")
    private String serviceProviderMetadataURL;

    @JsonProperty("serviceProviderEntityId")
    private String serviceProviderEntityId;

    public String getAssertionConsumerServiceURL() {
        return assertionConsumerServiceURL;
    }

    public void setAssertionConsumerServiceURL(String assertionConsumerServiceURL) {
        this.assertionConsumerServiceURL = assertionConsumerServiceURL;
    }

    public String getServiceProviderMetadataURL() {
        return serviceProviderMetadataURL;
    }

    public void setServiceProviderMetadataURL(String serviceProviderMetadataURL) {
        this.serviceProviderMetadataURL = serviceProviderMetadataURL;
    }

    public String getServiceProviderEntityId() {
        return serviceProviderEntityId;
    }

    public void setServiceProviderEntityId(String serviceProviderEntityId) {
        this.serviceProviderEntityId = serviceProviderEntityId;
    }

}
