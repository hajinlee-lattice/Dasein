package com.latticeengines.domain.exposed.saml;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModel;

@JsonIgnoreProperties(ignoreUnknown = true)
@ApiModel("Represents SP Initiated SamlLoginRequest JSON Object")
public class SpSamlLoginRequest {

    private String tenantDeploymentId;

    private Map<String, String> requestParameters;

    @JsonProperty(value = "tenantDeploymentId")
    public String getTenantDeploymentId() {
        return tenantDeploymentId;
    }

    public void setTenantDeploymentId(String tenantDeploymentId) {
        this.tenantDeploymentId = tenantDeploymentId;
    }

    @JsonProperty(value = "requestParameters")
    public Map<String, String> getRequestParameters() {
        return requestParameters;
    }

    public void setRequestParameters(Map<String, String> requestParameters) {
        this.requestParameters = requestParameters;
    }

}
