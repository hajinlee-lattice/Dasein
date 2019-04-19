package com.latticeengines.domain.exposed.security;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ResetPasswordConfirmationRequest {

    @JsonProperty("userEmail")
    private String userEmail;

    @JsonProperty("hostPort")
    private String hostPort;

    public ResetPasswordConfirmationRequest() {
    }

    public String getUserEmail() {
        return userEmail;
    }

    public void setUserEmail(String userEmail) {
        this.userEmail = userEmail;
    }

    public String getHostPort() {
        return hostPort;
    }

    public void setHostPort(String hostPort) {
        this.hostPort = hostPort;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
