package com.latticeengines.domain.exposed.auth;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.security.UserLanguage;

public class IDaaSExternalSession extends GlobalAuthExternalSession {

    public static final String TYPE = "IDaaS";

    @Override
    @JsonProperty("Type")
    public String getType() {
        return TYPE;
    }

    @JsonProperty("language")
    private UserLanguage language;

    public UserLanguage getLanguage() {
        return language;
    }

    public void setLanguage(UserLanguage language) {
        this.language = language;
    }
}
