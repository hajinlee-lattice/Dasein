package com.latticeengines.domain.exposed.pls;

import java.util.Map;

import org.codehaus.jackson.annotate.JsonProperty;

public class JwtRequestParameters {
    @JsonProperty
    private Map<String, String> requestParameters;

    public Map<String, String> getRequestParameters() {
        return requestParameters;
    }

    public void setRequestParameters(Map<String, String> map) {
        requestParameters = map;
    }
}
