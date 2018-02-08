package com.latticeengines.domain.exposed.ulysses;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_EMPTY)
public class GatewayPolicyConfiguration {

    @JsonProperty("principal")
    private String principal;
    
    @JsonProperty("apiKey")
    private String apiKey;

    //Can add allowed roles / policy configurations that can consumed on Gateway.
    //private List<String> roles
    
    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    public String getApiKey() {
        return apiKey;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }
    
}
