package com.latticeengines.domain.exposed.auth;

import com.fasterxml.jackson.annotation.JsonProperty;

public class IDaaSExternalSession extends GlobalAuthExternalSession {

    public static final String TYPE = "IDaaS";

    @Override
    @JsonProperty("Type")
    public String getType() {
        return TYPE;
    }

}
