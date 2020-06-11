package com.latticeengines.domain.exposed.remote.tray;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TraySettings {

    @JsonProperty("userToken")
    private String userToken;

    @JsonProperty("solutionInstanceId")
    private String solutionInstanceId;

    @JsonProperty("authenticationId")
    private String authenticationId;

    @JsonProperty("clientMutationId")
    private String clientMutationId;

    public TraySettings() {

    }

    public TraySettings(String userToken, String solutionInstanceId, String authenticationId, String clientMutationId) {
        this.userToken = userToken;
        this.solutionInstanceId = solutionInstanceId;
        this.authenticationId = authenticationId;
        this.clientMutationId = clientMutationId;
    }

    public void setUserToken(String userToken) {
        this.userToken = userToken;
    }

    public String getUserToken() {
        return this.userToken;
    }

    public void setSolutionInstanceId(String solutionInstanceId) {
        this.solutionInstanceId = solutionInstanceId;
    }

    public String getSolutionInstanceId() {
        return this.solutionInstanceId;
    }

    public void setAuthenticationId(String authenticationId) {
        this.authenticationId = authenticationId;
    }

    public String getAuthenticationId() {
        return this.authenticationId;
    }

    public void setClientMutationId(String clientMutationId) {
        this.clientMutationId = clientMutationId;
    }

    public String getClientMutationId() {
        return this.clientMutationId;
    }

    public static class TraySettingsBuilder {

        private String userToken;

        private String solutionInstanceId;

        private String authenticationId;

        private String clientMutationId;

        public TraySettingsBuilder userToken(String userToken) {
            this.userToken = userToken;
            return this;
        }

        public TraySettingsBuilder solutionInstanceId(String solutionInstanceId) {
            this.solutionInstanceId = solutionInstanceId;
            return this;
        }

        public TraySettingsBuilder authenticationId(String authenticationId) {
            this.authenticationId = authenticationId;
            return this;
        }

        public TraySettingsBuilder clientMutationId(String clientMutationId) {
            this.clientMutationId = clientMutationId;
            return this;
        }

        public TraySettings build() {
            return new TraySettings(this.userToken, this.solutionInstanceId, this.authenticationId,
                    this.clientMutationId);
        }

    }

}
