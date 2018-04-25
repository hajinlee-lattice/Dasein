package com.latticeengines.domain.exposed.security;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Credentials {

    private String username;
    private String password;

    @JsonProperty("Username")
    public String getUsername() {
        return username;
    }

    @JsonProperty("Username")
    public void setUsername(String username) {
        this.username = username;
    }

    @JsonProperty("Password")
    public String getPassword() {
        return password;
    }

    @JsonProperty("Password")
    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public String toString() {
        // make sure that when this object is logged, we hide user password
        // (even though it is encrypted). First clone this object and remove
        // password from that cloned obj
        Credentials credForLogging = JsonUtils.deserialize(JsonUtils.serialize(this), Credentials.class);
        credForLogging.setPassword("<<password_hidden>>");
        return JsonUtils.serialize(credForLogging);
    }
}
