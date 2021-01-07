package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AuthInvalidatedEventDetail extends EventDetail {

    public AuthInvalidatedEventDetail() {
        super("AuthInvalidated");
    }

    @JsonProperty("tray_auth_id")
    private String trayAuthenticationId;

    @JsonProperty("tray_user_id")
    private String trayUserId;

    @JsonProperty("env_name")
    private String envName;

    public String getTrayAuthenticationId() {
        return trayAuthenticationId;
    }

    public void setTrayAuthenticationId(String trayAuthenticationId) {
        this.trayAuthenticationId = trayAuthenticationId;
    }

    public String getTrayUserId() {
        return trayUserId;
    }

    public void setTrayUserId(String trayUserId) {
        this.trayUserId = trayUserId;
    }

    public String getEnvName() {
        return envName;
    }

    public void setEnvName(String envName) {
        this.envName = envName;
    }
}
