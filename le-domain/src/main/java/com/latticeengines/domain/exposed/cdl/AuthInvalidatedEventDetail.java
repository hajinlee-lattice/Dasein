package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AuthInvalidatedEventDetail extends EventDetail {

    public AuthInvalidatedEventDetail() {
        super("AuthInvalidated");
    }

    @JsonProperty("tray_authentication_id")
    private String trayAuthenticationId;

    @JsonProperty("tray_user_id")
    private String trayUserId;

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

}
