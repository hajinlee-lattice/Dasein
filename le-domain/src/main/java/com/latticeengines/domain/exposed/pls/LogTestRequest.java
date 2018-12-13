package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LogTestRequest {

    @JsonProperty
    public String source;

    @JsonProperty
    public String gaToken;

    @JsonProperty
    public String oauthToken;

}
